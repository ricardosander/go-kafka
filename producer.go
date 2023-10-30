package main

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"log"
)

func startProducer(kafkaAddress []string, kafkaTopics []string) {
	log.Print("Starting producer")

	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(
		kafkaAddress,
		config,
	)
	if err != nil {
		log.Fatalln("Fail to start producer", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Fail while closing producer: %v", err)
		}
	}()

	for _, topicName := range kafkaTopics {

		message := NewMessage()
		jsonMessage, err := json.Marshal(message)
		if err != nil {
			log.Printf("Fail to marshal message: %v", err)
		}
		msg := &sarama.ProducerMessage{
			Topic: topicName,
			Value: sarama.StringEncoder(jsonMessage),
			Key:   sarama.StringEncoder(message.Id),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Fatalln("Fail to send message", err)
		}
		log.Printf("Message sent! Topic = %s,Partition = %d, Offset = %d\n", topicName, partition, offset)
	}
}
