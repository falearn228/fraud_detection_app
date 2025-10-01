package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"fraud-detection-app/internal/kafka"
	"fraud-detection-app/internal/models"

	"github.com/IBM/sarama"
)

// TransactionWindow представляет окно транзакций для пользователя
type TransactionWindow struct {
	UserID       int64
	WindowStart  time.Time
	WindowEnd    time.Time
	Count        int
	MaxAmount    float64 // Максимальная сумма транзакции в окне
	TotalAmount  float64 // Общая сумма всех транзакций в окне
	Transactions []*models.Transaction
}

// FraudStreamsService реализует Kafka Streams аналог для обнаружения мошенничества
type FraudStreamsService struct {
	consumer     sarama.Consumer
	producer     kafka.Producer
	topic        string        // 1. Конфигурация входов - INPUT_TOPIC
	resultTopic  string        // 1. Конфигурация выходов - OUTPUT_TOPIC
	windows      map[string]*TransactionWindow // key: userId_windowStart
	windowsMutex sync.RWMutex
	windowSize   time.Duration // 1. Конфигурация - WINDOW_SIZE (5 минут)
	maxCount     int           // Порог для обнаружения мошенничества (>= 3)
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewFraudStreamsService создает новый сервис streams
func NewFraudStreamsService(kafkaConfig *sarama.Config, bootstrapServers []string, producer kafka.Producer, topic, resultTopic string) (*FraudStreamsService, error) {
	consumer, err := sarama.NewConsumer(bootstrapServers, kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &FraudStreamsService{
		consumer:    consumer,
		producer:    producer,
		topic:       topic,
		resultTopic: resultTopic,
		windows:     make(map[string]*TransactionWindow),
		windowSize:  5 * time.Minute, 
		maxCount:    3,               
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

// Start начинает обработку streams
func (fss *FraudStreamsService) Start() error {
	partitionConsumer, err := fss.consumer.ConsumePartition(fss.topic, 0, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("failed to create partition consumer: %w", err)
	}

	log.Printf("Started consuming from topic: %s", fss.topic)

	go func() {
		defer partitionConsumer.Close()

		for {
			select {
			case <-fss.ctx.Done():
				log.Println("Stopping fraud streams service")
				return
			case msg := <-partitionConsumer.Messages():
				fss.processMessage(msg)
			case err := <-partitionConsumer.Errors():
				log.Printf("Consumer error: %v", err)
			}
		}
	}()

	// Запускаем очистку окон каждые 30 секунд
	go fss.cleanupWindows()

	return nil
}

// Stop останавливает сервис
func (fss *FraudStreamsService) Stop() {
	fss.cancel()
	fss.consumer.Close()
}

// processMessage обрабатывает входящее сообщение
func (fss *FraudStreamsService) processMessage(msg *sarama.ConsumerMessage) {
	// 2. Десериализация транзакций
	var transaction models.Transaction

	if err := json.Unmarshal(msg.Value, &transaction); err != nil {
		log.Printf("Failed to unmarshal transaction: %v", err)
		return
	}

	fss.processTransaction(&transaction)
}

// processTransaction обрабатывает транзакцию в оконном режиме
func (fss *FraudStreamsService) processTransaction(tx *models.Transaction) {
	// 3. Фильтрация и обработка - только NEW транзакции
	if tx.Status != "NEW" {
		return
	}

	fss.windowsMutex.Lock()
	defer fss.windowsMutex.Unlock()

	now := time.Now()
	windowStart := now.Truncate(fss.windowSize)
	windowEnd := windowStart.Add(fss.windowSize)

	windowKey := fmt.Sprintf("%d_%d", tx.UserID, windowStart.Unix())

	window, exists := fss.windows[windowKey]
	if !exists {
		window = &TransactionWindow{
			UserID:       tx.UserID,
			WindowStart:  windowStart,
			WindowEnd:    windowEnd,
			Count:        0,
			MaxAmount:    0,
			TotalAmount:  0,
			Transactions: make([]*models.Transaction, 0),
		}
		fss.windows[windowKey] = window
	}

	// Добавляем транзакцию в окно
	window.Count++
	window.TotalAmount += tx.Amount
	if tx.Amount > window.MaxAmount {
		window.MaxAmount = tx.Amount
	}
	window.Transactions = append(window.Transactions, tx)

	log.Printf("User %d: transaction count in window %d-%d: %d, max: %.2f, avg: %.2f",
		tx.UserID, windowStart.Unix(), windowEnd.Unix(), window.Count,
		window.MaxAmount, window.TotalAmount/float64(window.Count))

	// 4. Генерация алертов - срабатывает при:
	// - MaxAmount > 5000 ИЛИ
	// - 3+ транзакции со средней суммой > 2000
	avgAmount := window.TotalAmount / float64(window.Count)
	if window.MaxAmount > 5000 || (window.Count >= fss.maxCount && avgAmount > 2000) {
		fss.detectFraud(window)
	}
}

// detectFraud обнаруживает мошенничество и отправляет результат
func (fss *FraudStreamsService) detectFraud(window *TransactionWindow) {
	avgAmount := window.TotalAmount / float64(window.Count)

	// Формируем детальную причину обнаружения
	var reason string
	if window.MaxAmount > 5000 {
		reason = fmt.Sprintf("High amount transaction: max=%.2f (>5000), count=%d, avg=%.2f",
			window.MaxAmount, window.Count, avgAmount)
	} else {
		reason = fmt.Sprintf("High frequency with high average: count=%d (>=3), avg=%.2f (>2000), max=%.2f",
			window.Count, avgAmount, window.MaxAmount)
	}

	result := FraudDetectionResult{
		TransactionID: "", // Для streams анализа используем пустой ID
		IsFraudulent:  true,
		RiskScore:     (float64(window.Count) * 0.3) + (window.MaxAmount / 10000), // Учитываем сумму и частоту
		Reason:        reason,
	}

	resultJSON, err := json.Marshal(result)
	if err != nil {
		log.Printf("Failed to marshal fraud result: %v", err)
		return
	}

	msg := &kafka.Message{
		Topic: fss.resultTopic,
		Key:   fmt.Sprintf("user_%d", window.UserID),
		Value: string(resultJSON),
		Headers: map[string]string{
			"event_type":        "fraud_detection_stream",
			"source":            "fraud-streams-service",
			"user_id":           fmt.Sprintf("%d", window.UserID),
			"window_start":      fmt.Sprintf("%d", window.WindowStart.Unix()),
			"transaction_count": fmt.Sprintf("%d", window.Count),
		},
	}

	if err := fss.producer.SendMessage(context.Background(), msg); err != nil {
		log.Printf("Failed to send fraud detection result: %v", err)
		return
	}

	log.Printf("Fraud detected for user %d: %s", window.UserID, result.Reason)
}

// cleanupWindows очищает устаревшие окна
func (fss *FraudStreamsService) cleanupWindows() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-fss.ctx.Done():
			return
		case <-ticker.C:
			fss.windowsMutex.Lock()
			now := time.Now()

			for key, window := range fss.windows {
				// Удаляем окна, которые закончились более 5 минут назад
				if now.After(window.WindowEnd.Add(5 * time.Minute)) {
					delete(fss.windows, key)
					log.Printf("Cleaned up window for user %d", window.UserID)
				}
			}

			fss.windowsMutex.Unlock()
		}
	}
}
