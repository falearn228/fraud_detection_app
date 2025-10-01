package main

import (
	"log"
	"os"

	"fraud-detection-app/internal/config"
	"fraud-detection-app/internal/controllers"
	"fraud-detection-app/internal/kafka"
	"fraud-detection-app/internal/repositories"
	"fraud-detection-app/internal/services"

	"github.com/gin-gonic/gin"
)

func main() {
	// Читаем переменные окружения
	debeziumTopic := getEnv("DEBEZIUM_TRANSACTION_TOPIC", "fraud-detection.public.transaction")
	fraudResultsTopic := getEnv("FRAUD_RESULTS_TOPIC", "fraud-detection-results")
	fraudStreamResultsTopic := getEnv("FRAUD_STREAM_RESULTS_TOPIC", "fraud-detection-stream-results")
	kafkaServers := getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

	// Конфигурация Kafka
	kafkaConfig := config.NewKafkaConfig([]string{kafkaServers})

	// Настройка SSL/TLS если указаны параметры
	kafkaConfig.SecurityProtocol = getEnv("KAFKA_SECURITY_PROTOCOL", "")
	kafkaConfig.SSLCALocation = getEnv("KAFKA_SSL_CA_LOCATION", "")
	kafkaConfig.SSLCertLocation = getEnv("KAFKA_SSL_CERT_LOCATION", "")
	kafkaConfig.SSLKeyLocation = getEnv("KAFKA_SSL_KEY_LOCATION", "")

	// Конфигурация Sarama для consumer
	saramaConfig := kafkaConfig.NewSaramaConfig()
	saramaConfig.Consumer.Return.Errors = true

	// Создаем Kafka producer
	producer, err := kafka.NewSaramaProducer(kafkaConfig)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Создаем сервис обнаружения мошенничества (простой анализ)
	fraudService := services.NewFraudDetectionService(producer, fraudResultsTopic)

	// Создаем Kafka Streams сервис (оконный анализ частоты)
	streamsService, err := services.NewFraudStreamsService(
		saramaConfig,
		kafkaConfig.BootstrapServers,
		producer,
		debeziumTopic,           // Топик от Debezium
		fraudStreamResultsTopic, // Выходной топик
	)
	if err != nil {
		log.Fatalf("Failed to create fraud streams service: %v", err)
	}
	defer streamsService.Stop()

	// Запускаем streams сервис
	if err := streamsService.Start(); err != nil {
		log.Fatalf("Failed to start fraud streams service: %v", err)
	}

	// Подключаемся к базе данных
	dbConfig := config.NewDatabaseConfig()
	db, err := dbConfig.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Создаем PostgreSQL репозиторий
	repo := repositories.NewPostgresTransactionRepository(db)

	// Инициализируем схему базы данных
	if postgresRepo, ok := repo.(*repositories.PostgresTransactionRepository); ok {
		if err := postgresRepo.InitSchema(); err != nil {
			log.Fatalf("Failed to initialize database schema: %v", err)
		}
	}

	// Создаем контроллер
	controller := controllers.NewTransactionController(repo, fraudService)

	// Настраиваем Gin роутер
	r := gin.Default()

	// API маршруты
	r.POST("/transactions", controller.CreateTransaction)

	// Запускаем сервер
	log.Println("Starting fraud detection service on :8080")
	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

// getEnv получает значение переменной окружения или возвращает значение по умолчанию
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
