package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
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
	flag.StringVar(&kafkaServerAddress, "ksa", "localhost", "Kafka Server Address")
	flag.IntVar(&gorutinesNumber, "gn", 2, "Gorutines number")
}

func processMessage(message *MessageStructure, jsonEncoder *json.Encoder) {
	encodeErr := jsonEncoder.Encode(&message.Message)
	if encodeErr != nil {
		panic(encodeErr)
	}
}

func processMessages(messages *[]MessageStructure, responseWriter *http.ResponseWriter) {
	jsonEncoder := json.NewEncoder(*responseWriter)
	for _, messageToSend := range *messages {
		processMessage(&messageToSend, jsonEncoder)
	}
}

var requestHandler = func (w http.ResponseWriter, req *http.Request)  {
	jsonDecoder := json.NewDecoder(req.Body)
	var decodedMessage RequestStructure
	decodeErr := jsonDecoder.Decode(&decodedMessage)
	if decodeErr != nil {
		panic(decodeErr)
	}
	processMessages(&decodedMessage.Messages, &w)
}

func main() {
	flag.Parse()
	fmt.Printf("Working with kafka server %s\n", kafkaServerAddress)
	fmt.Printf("Working with %d gorutines\n", gorutinesNumber)

	http.HandleFunc("/send", requestHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
