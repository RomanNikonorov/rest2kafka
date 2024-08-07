package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/IBM/sarama"
)

var (
	userName   = flag.String("username", "", "The SASL username")
	passwd     = flag.String("passwd", "", "The SASL password")
	brokers    = flag.String("brokers", "localhost:9092", "The Kafka brokers to connect to, as a comma separated list")
	encription = flag.String("encription", "256", "Encription mode 256 or 512")
)

func processMessages(messages *[]MessageStructure, responseWriter *http.ResponseWriter, topic string) string {
	//jsonEncoder := json.NewEncoder(*responseWriter)
	conf := sarama.NewConfig()
	conf.Producer.Retry.Max = 1
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Successes = true
	conf.Metadata.Full = true
	conf.Version = sarama.V3_6_0_0

	if *userName != "" && *passwd != "" {
		conf.Net.SASL.Enable = true
		conf.Net.SASL.User = *userName
		conf.Net.SASL.Password = *passwd
		conf.Net.SASL.Handshake = true
	}

	if *encription == "256" {
		conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		conf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	} else if *encription == "512" {
		conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
		conf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	}
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

	messagesToSend := prepareMessages(messages, topic)
	sendingError := producer.SendMessages(messagesToSend)
	if sendingError != nil {
		var responseErr string
		for _, e := range sendingError.(sarama.ProducerErrors) {
			log.Printf("FAILED to send message: %s\n", e)
			responseErr = responseErr + e.Err.Error() + "\n"
		}
		return responseErr
	} else {
		log.Printf("Messages sent")
		return "Sent"
	}
}

var requestHandler = func(w http.ResponseWriter, req *http.Request) {
	jsonDecoder := json.NewDecoder(req.Body)
	var decodedMessage RequestStructure
	decodeErr := jsonDecoder.Decode(&decodedMessage)
	if decodeErr != nil {
		panic(decodeErr)
	}
	resp := processMessages(&decodedMessage.Messages, &w, decodedMessage.Topic)
	_, err := w.Write([]byte(resp))
	if err != nil {
		return
	}
}

func main() {
	flag.Parse()
	fmt.Printf("Working with brokers %s\n", *brokers)
	http.HandleFunc("/send", requestHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
