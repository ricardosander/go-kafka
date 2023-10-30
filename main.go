package main

import "os"

func main() {
	kafkaAddress := []string{"localhost:9092"}
	kafkaTopics := []string{"test-topic-message"}
	appName := "goKafka"

	args := os.Args[1:]
	isStartConsumer := false
	isStartProducer := false
	for _, value := range args {
		if value == "consume" {
			isStartConsumer = true
		}
		if value == "produce" {
			isStartProducer = true
		}
	}

	if isStartProducer {
		startProducer(kafkaAddress, kafkaTopics)
	}
	if isStartConsumer {
		startConsumer(kafkaAddress, kafkaTopics, appName)
	}
}
