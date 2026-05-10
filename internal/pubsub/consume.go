package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

const (
	Ack Acktype = iota
	NackDiscard
	NackRequeue
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create channel: %v", err)
	}

	queue, err := ch.QueueDeclare(
		queueName,                       // name
		queueType == SimpleQueueDurable, // durable
		queueType != SimpleQueueDurable, // delete when unused
		queueType != SimpleQueueDurable, // exclusive
		false,                           // no-wait
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		}, // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare queue: %v", err)
	}

	err = ch.QueueBind(
		queue.Name, // queue name
		key,        // routing key
		exchange,   // exchange
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not bind queue: %v", err)
	}
	return ch, queue, nil
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) Acktype) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could not declare and bind: %v", err)
	}

	consumeChannel, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not consume from queue: %v", err)
	}

	go func() {
		for d := range consumeChannel {
			var body T
			err := json.Unmarshal(d.Body, &body)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				d.Nack(false, false)
				continue
			}
			switch handler(body) {
			case Ack:
				d.Ack(false)
				fmt.Println("Ack")
			case NackDiscard:
				d.Nack(false, false)
				fmt.Println("NackDiscard")
			case NackRequeue:
				d.Nack(false, true)
				fmt.Println("NackRequeue")
			}
		}
	}()

	return nil
}

func SubscribeGob[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) Acktype) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could not declare and bind: %v", err)
	}

	consumeChannel, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not consume from queue: %v", err)
	}

	go func() {
		for d := range consumeChannel {
			reader := bytes.NewReader(d.Body)

			decoder := gob.NewDecoder(reader)

			var body T

			err := decoder.Decode(&body)
			if err != nil {
				fmt.Printf("Decode error: %v\n", err)
				d.Nack(false, false)
				continue
			}
			switch handler(body) {
			case Ack:
				d.Ack(false)
				fmt.Println("Ack")
			case NackDiscard:
				d.Nack(false, false)
				fmt.Println("NackDiscard")
			case NackRequeue:
				d.Nack(false, true)
				fmt.Println("NackRequeue")
			}
		}
	}()

	return nil
}
