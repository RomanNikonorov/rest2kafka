package main

import "github.com/Shopify/sarama"

func prepareMessage(message *MessageStructure, topic string) *sarama.ProducerMessage {
	messageString := string(message.Message)
	producerMessage := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(messageString)}
	return producerMessage
}

func prepareMessages(messages *[]MessageStructure, topic string) []*sarama.ProducerMessage {
	result := make([]*sarama.ProducerMessage, len(*messages))
	for i, message := range *messages {
		result[i] = prepareMessage(&message, topic)
	}
	return result
}
