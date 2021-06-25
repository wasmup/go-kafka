package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "my-topic"
	partition := 0
	{
		// to create topics when auto.create.topics.enable='true'
		conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
		if err != nil {
			log.Fatal(err)
		}

		// to create topics when auto.create.topics.enable='false'
		// conn, err := kafka.Dial("tcp", "localhost:9092")
		// if err != nil {
		// 	log.Fatal(err)
		// }
		// defer conn.Close()

		controller, err := conn.Controller()
		if err != nil {
			log.Fatal(err)
		}

		var controllerConn *kafka.Conn
		controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
		if err != nil {
			log.Fatal(err)
		}
		topicConfigs := []kafka.TopicConfig{{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, {
			Topic:             "topic-A",
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, {
			Topic:             "topic-B",
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, {
			Topic:             "topic-C",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
		}
		err = controllerConn.CreateTopics(topicConfigs...)
		if err != nil {
			log.Fatal(err)
		}
		conn.Close()
		controllerConn.Close()
	}

	{
		// produce messages
		conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
		if err != nil {
			log.Fatal("failed to dial leader:", err)
		}

		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		_, err = conn.WriteMessages(
			kafka.Message{Value: []byte("one!")},
			kafka.Message{Value: []byte("two!")},
			kafka.Message{Value: []byte("three!")},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}

		if err := conn.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}
		conn.Close()
	}
	{
		// list topics
		conn, err := kafka.Dial("tcp", "localhost:9092")
		if err != nil {
			log.Fatal(err)
		}
		partitions, err := conn.ReadPartitions()
		if err != nil {
			log.Fatal(err)
		}
		m := map[string]struct{}{}

		for _, p := range partitions {
			m[p.Topic] = struct{}{}
		}
		for k := range m {
			fmt.Println(k)
		}
		conn.Close()
	}
	{
		w := &kafka.Writer{
			Addr: kafka.TCP("localhost:9092"),
			// NOTE: When Topic is not defined here, each Message must define it instead.
			Balancer: &kafka.LeastBytes{},
		}

		err := w.WriteMessages(context.Background(),
			// NOTE: Each Message has Topic defined, otherwise an error is returned.
			kafka.Message{
				Topic: "topic-A",
				Key:   []byte("Key-A"),
				Value: []byte("Hello World!"),
			},
			kafka.Message{
				Topic: "topic-B",
				Key:   []byte("Key-B"),
				Value: []byte("One!"),
			},
			kafka.Message{
				Topic: "topic-C",
				Key:   []byte("Key-C"),
				Value: []byte("Two!"),
			},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}

		if err := w.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}
	}
	{
		// make a new reader that consumes from topic-A, partition 0, at offset 42
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{"localhost:9092"},
			Topic:     "topic-A",
			Partition: 0,
			MinBytes:  10e3, // 10KB
			MaxBytes:  10e6, // 10MB
		})
		// r.SetOffset(42)

		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				break
			}
			fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
		}

		if err := r.Close(); err != nil {
			log.Fatal("failed to close reader:", err)
		}
	}
}
