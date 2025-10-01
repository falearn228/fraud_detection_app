package controllers

import (
	"context"
	"fraud-detection-app/internal/models"
	"fraud-detection-app/internal/repositories"
	"fraud-detection-app/internal/services"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// TransactionController обрабатывает HTTP запросы для транзакций
type TransactionController struct {
	repository            repositories.TransactionRepository
	fraudDetectionService *services.FraudDetectionService
}

// NewTransactionController создает новый экземпляр TransactionController
func NewTransactionController(repository repositories.TransactionRepository, fraudDetectionService *services.FraudDetectionService) *TransactionController {
	return &TransactionController{
		repository:            repository,
		fraudDetectionService: fraudDetectionService,
	}
}

// CreateTransaction обрабатывает POST /transactions
func (tc *TransactionController) CreateTransaction(c *gin.Context) {
	var transaction models.Transaction

	if err := c.ShouldBindJSON(&transaction); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Генерируем новый UUID для транзакции
	transaction.ID = uuid.New()
	transaction.Status = models.StatusNew
	transaction.Timestamp = time.Now()

	savedTransaction, err := tc.repository.Save(&transaction)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save transaction"})
		return
	}

	// Анализируем транзакцию на мошенничество
	if tc.fraudDetectionService != nil {
		go func() {
			ctx := context.Background()
			result, err := tc.fraudDetectionService.AnalyzeTransaction(ctx, savedTransaction)
			if err != nil {
				// Логируем ошибку, но не прерываем ответ клиенту
				log.Printf("Failed to analyze transaction: %v", err)
				return
			}

			// Обновляем статус транзакции на основе результата анализа
			if result.IsFraudulent {
				savedTransaction.Status = models.StatusSuspicious
				// Сохраняем обновленный статус
				if _, err := tc.repository.Save(savedTransaction); err != nil {
					log.Printf("Failed to update transaction status: %v", err)
				}
			}
		}()
	}

	c.JSON(http.StatusCreated, savedTransaction)
}
