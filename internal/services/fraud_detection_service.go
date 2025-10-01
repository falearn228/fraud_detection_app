package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"fraud-detection-app/internal/kafka"
	"fraud-detection-app/internal/models"
)

// FraudDetectionResult результат анализа транзакции на мошенничество
type FraudDetectionResult struct {
	TransactionID string  `json:"transaction_id"`
	IsFraudulent  bool    `json:"is_fraudulent"`
	RiskScore     float64 `json:"risk_score"`
	Reason        string  `json:"reason,omitempty"`
}

// FraudDetectionService сервис для обнаружения мошенничества
type FraudDetectionService struct {
	producer kafka.Producer
	topic    string
}

// NewFraudDetectionService создает новый сервис обнаружения мошенничества
func NewFraudDetectionService(producer kafka.Producer, topic string) *FraudDetectionService {
	return &FraudDetectionService{
		producer: producer,
		topic:    topic,
	}
}

// AnalyzeTransaction анализирует транзакцию на мошенничество
func (fds *FraudDetectionService) AnalyzeTransaction(ctx context.Context, transaction *models.Transaction) (*FraudDetectionResult, error) {
	// Простая логика обнаружения мошенничества
	result := &FraudDetectionResult{
		TransactionID: transaction.ID.String(),
		RiskScore:     0.0,
		IsFraudulent:  false,
	}

	// Пример правил обнаружения мошенничества:
	// 1. Сумма > 10000 - высокий риск
	if transaction.Amount > 10000 {
		result.RiskScore += 0.8
		result.Reason = "High transaction amount"
	}

	// 2. Статус NEW - подозрительно
	if transaction.Status == models.StatusNew {
		result.RiskScore += 0.2
		if result.Reason != "" {
			result.Reason += "; "
		}
		result.Reason += "New transaction status"
	}

	// 3. Если общий риск > 0.7 - считаем мошеннической
	if result.RiskScore > 0.7 {
		result.IsFraudulent = true
	}

	// Отправляем результат в Kafka
	if err := fds.sendResultToKafka(ctx, result); err != nil {
		log.Printf("Failed to send fraud detection result to Kafka: %v", err)
		// Не возвращаем ошибку, чтобы не прерывать основной поток
	}

	return result, nil
}

// sendResultToKafka отправляет результат анализа в Kafka
func (fds *FraudDetectionService) sendResultToKafka(ctx context.Context, result *FraudDetectionResult) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}

	msg := &kafka.Message{
		Topic: fds.topic,
		Key:   result.TransactionID,
		Value: string(resultJSON),
		Headers: map[string]string{
			"event_type": "fraud_detection_result",
			"source":     "fraud-detection-service",
		},
	}

	return fds.producer.SendMessage(ctx, msg)
}
