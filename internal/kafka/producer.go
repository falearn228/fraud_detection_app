package kafka

import (
	"context"
	"fmt"
	"log"

	"fraud-detection-app/internal/config"

	"github.com/IBM/sarama"
)

// Message представляет сообщение Kafka
type Message struct {
	Topic   string
	Key     string
	Value   string
	Headers map[string]string
}

// Producer интерфейс для отправки сообщений в Kafka
type Producer interface {
	SendMessage(ctx context.Context, msg *Message) error
	Close() error
}

// SaramaProducer реализация Producer с использованием Sarama
type SaramaProducer struct {
	producer sarama.SyncProducer
}

// NewSaramaProducer создает новый Kafka producer
func NewSaramaProducer(kafkaConfig *config.KafkaConfig) (Producer, error) {
	if err := kafkaConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid kafka config: %w", err)
	}

	saramaConfig := kafkaConfig.NewSaramaConfig()

	producer, err := sarama.NewSyncProducer(kafkaConfig.BootstrapServers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	return &SaramaProducer{
		producer: producer,
	}, nil
}

// SendMessage отправляет сообщение в Kafka
func (p *SaramaProducer) SendMessage(ctx context.Context, msg *Message) error {
	saramaMsg := &sarama.ProducerMessage{
		Topic: msg.Topic,
		Key:   sarama.StringEncoder(msg.Key),
		Value: sarama.StringEncoder(msg.Value),
	}

	// Добавляем заголовки если они есть
	if len(msg.Headers) > 0 {
		saramaMsg.Headers = make([]sarama.RecordHeader, 0, len(msg.Headers))
		for k, v := range msg.Headers {
			saramaMsg.Headers = append(saramaMsg.Headers, sarama.RecordHeader{
				Key:   []byte(k),
				Value: []byte(v),
			})
		}
	}

	partition, offset, err := p.producer.SendMessage(saramaMsg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	log.Printf("Message sent to topic %s partition %d offset %d", msg.Topic, partition, offset)
	return nil
}

// Close закрывает producer
func (p *SaramaProducer) Close() error {
	return p.producer.Close()
}
