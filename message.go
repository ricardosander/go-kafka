package main

import (
	"github.com/hashicorp/go-uuid"
	"math/rand"
	"time"
)

type Message struct {
	Id         string  `json:"id"`
	Date       string  `json:"date"`
	CustomerId string  `json:"customer_id"`
	Type       string  `json:"type"`
	Value      float64 `json:"value"`
}

func NewMessage() Message {
	id, _ := uuid.GenerateUUID()
	customerId, _ := uuid.GenerateUUID()
	return Message{
		Id:         id,
		Date:       time.Now().Format("2006-1-2"),
		CustomerId: customerId,
		Type:       "price",
		Value:      rand.Float64(),
	}
}
