package main

import (
	"crypto/sha256"
	"crypto/sha512"

	"github.com/xdg-go/scram"
	
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/Shopify/sarama"
)

type MessageStructure struct {
	Message json.RawMessage `json:"message"`
	Header  string          `json:"header"`
}

type RequestStructure struct {
	Topic    string             `json:"topic"`
	Messages []MessageStructure `json:"messages"`
}

var (
	userName = flag.String("username", "", "The SASL username")
	passwd   = flag.String("passwd", "", "The SASL password")
	brokers  = flag.String("ksa", "localhost:9092", "The Kafka brokers to connect to, as a comma separated list")
)

func init() {

}

var (
	SHA256 scram.HashGeneratorFcn = sha256.New
	SHA512 scram.HashGeneratorFcn = sha512.New
)

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

func processMessage(message *MessageStructure, jsonEncoder *json.Encoder, topic string) {
	encodeErr := jsonEncoder.Encode(&message.Message)
	if encodeErr != nil {
		panic(encodeErr)
	}

	conf := sarama.NewConfig()
	conf.Producer.Retry.Max = 1
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Successes = true
	conf.Metadata.Full = true
	conf.Version = sarama.V0_10_0_0
	conf.Net.SASL.Enable = true
	conf.Net.SASL.User = *userName
	conf.Net.SASL.Password = *passwd
	conf.Net.SASL.Handshake = true
	conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
	conf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	conf.ClientID = "rest2kafka"

	splitBrokers := strings.Split(*brokers, ",")
	producer, err := sarama.NewSyncProducer(splitBrokers, conf)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	str := string(message.Message)
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(str)}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("FAILED to send message: %s\n", err)
	} else {
		log.Printf("> message sent to partition %d at offset %d\n", partition, offset)
	}
}

func processMessages(messages *[]MessageStructure, responseWriter *http.ResponseWriter, topic string) {
	jsonEncoder := json.NewEncoder(*responseWriter)
	for _, messageToSend := range *messages {
		processMessage(&messageToSend, jsonEncoder, topic)
	}
}

var requestHandler = func(w http.ResponseWriter, req *http.Request) {
	jsonDecoder := json.NewDecoder(req.Body)
	var decodedMessage RequestStructure
	decodeErr := jsonDecoder.Decode(&decodedMessage)
	if decodeErr != nil {
		panic(decodeErr)
	}
	processMessages(&decodedMessage.Messages, &w, decodedMessage.Topic)
}

func main() {
	flag.Parse()
	fmt.Printf("Working with brokers %s\n", *brokers)
	http.HandleFunc("/send", requestHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
