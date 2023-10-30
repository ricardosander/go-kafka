package main

import (
	"context"
	"encoding/json"
	"github.com/IBM/sarama"
	"log"
	"time"
)

func startConsumer(kafkaAddress []string, kafkaTopics []string, consumerGroup string) {
	log.Print("Starting consumer")

	config := sarama.NewConfig()

	group, err := sarama.NewConsumerGroup(
		kafkaAddress,
		consumerGroup,
		config,
	)
	if err != nil {
		log.Fatalln("Fail to create consumer group", err)
	}
	defer func() {
		if err := group.Close(); err != nil {
			log.Printf("Fail while closing producer group: %v", err)
		}
	}()

	ctx := context.Background()
	for {
		handler := myConsumer{}
		err := group.Consume(ctx, kafkaTopics, handler)
		if err != nil {
			log.Fatalln("Fail to consume from consumer group", err)
		}
	}
}

func main2(kafkaAddress []string, kafkaTopics []string) {
	consumer, err := sarama.NewConsumer(kafkaAddress, nil)
	if err != nil {
		log.Fatalln("Fail to start consumer", err)
	}

	for _, topic := range kafkaTopics {
		partitions, err := consumer.Partitions(topic)
		if err != nil {
			log.Fatalln("Fail to retrieve partitions", err)
		}

		for partitionIndex, partition := range partitions {
			log.Printf("Partintion index: %v\n", partitionIndex)

			pc, err := consumer.ConsumePartition(
				topic,
				partition,
				sarama.OffsetOldest,
			)
			if err != nil {
				log.Fatalln("Fail to start consumer to partition", err)
			}
			go func(pc sarama.PartitionConsumer) {
				for message := range pc.Messages() {
					log.Printf("Recieve message %v - %v\n", string(message.Key), string(message.Value))
				}
			}(pc)
		}
	}

	for {
		time.Sleep(time.Second * 30)
		log.Println("Waiting messages")
	}
}

type myConsumer struct{}

func (myConsumer) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (myConsumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h myConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var message Message
		err := json.Unmarshal(msg.Value, &message)
		if err != nil {
			log.Printf("Fail unmarshaling message: %v\n", err)
		}
		log.Printf("%+v", message)
		log.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		sess.MarkMessage(msg, "")
	}
	return nil
}
