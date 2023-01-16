package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	sarama "gopkg.in/Shopify/sarama.v1"
)

var kafkaServerAddress string
var gorutinesNumber int

type MessageStructure struct {
	Message json.RawMessage `json:"message"`
	Header string `json:"header"`
}

type RequestStructure struct {
	Topic string `json:"topic"` 
	Messages []MessageStructure `json:"messages"`
}

func init() {
	flag.StringVar(&kafkaServerAddress, "ksa", "localhost:9092", "Kafka Server Address")
	flag.IntVar(&gorutinesNumber, "gn", 2, "Gorutines number")
}

func processMessage(message *MessageStructure, jsonEncoder *json.Encoder, topic string) {
	encodeErr := jsonEncoder.Encode(&message.Message)
	if encodeErr != nil {
		panic(encodeErr)
	}

	producer, err := sarama.NewSyncProducer([]string{kafkaServerAddress}, nil)
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

var requestHandler = func (w http.ResponseWriter, req *http.Request)  {
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
	fmt.Printf("Working with kafka server %s\n", kafkaServerAddress)
	fmt.Printf("Working with %d gorutines\n", gorutinesNumber)

	http.HandleFunc("/send", requestHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
