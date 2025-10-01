package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fraud-detection-app/internal/kafka"
	"fraud-detection-app/internal/models"
	"fraud-detection-app/internal/services"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockTransactionRepository мок для TransactionRepository
type MockTransactionRepository struct {
	mock.Mock
}

func (m *MockTransactionRepository) Save(transaction *models.Transaction) (*models.Transaction, error) {
	args := m.Called(transaction)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.Transaction), args.Error(1)
}

func (m *MockTransactionRepository) FindByID(id uuid.UUID) (*models.Transaction, error) {
	args := m.Called(id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.Transaction), args.Error(1)
}

func (m *MockTransactionRepository) FindAll() ([]*models.Transaction, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*models.Transaction), args.Error(1)
}

// MockProducer мок для Kafka Producer
type MockProducer struct {
	mock.Mock
}

func (m *MockProducer) SendMessage(ctx context.Context, msg *kafka.Message) error {
	args := m.Called(ctx, msg)
	return args.Error(0)
}

func (m *MockProducer) Close() error {
	args := m.Called()
	return args.Error(0)
}

// setupTestRouter настраивает тестовый роутер
func setupTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	return gin.Default()
}

func TestCreateTransaction_Success(t *testing.T) {
	// Arrange
	mockRepo := new(MockTransactionRepository)
	mockProducer := new(MockProducer)
	fraudService := services.NewFraudDetectionService(mockProducer, "test-topic")
	controller := NewTransactionController(mockRepo, fraudService)

	router := setupTestRouter()
	router.POST("/transactions", controller.CreateTransaction)

	// Подготовка тестовых данных
	requestBody := map[string]interface{}{
		"amount":   150.0,
		"currency": "USD",
		"user_id":  123,
	}
	jsonBody, _ := json.Marshal(requestBody)

	// Ожидаемая транзакция после сохранения
	expectedTransaction := &models.Transaction{
		ID:        uuid.New(),
		Amount:    150.0,
		Currency:  "USD",
		UserID:    123,
		Status:    models.StatusNew,
		Timestamp: time.Now(),
	}

	mockRepo.On("Save", mock.AnythingOfType("*models.Transaction")).Return(expectedTransaction, nil)
	mockProducer.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	// Act
	req, _ := http.NewRequest("POST", "/transactions", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assert
	assert.Equal(t, http.StatusCreated, w.Code)

	var response models.Transaction
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, expectedTransaction.Amount, response.Amount)
	assert.Equal(t, expectedTransaction.Currency, response.Currency)
	assert.Equal(t, expectedTransaction.UserID, response.UserID)
	assert.Equal(t, models.StatusNew, response.Status)

	mockRepo.AssertExpectations(t)
}

func TestCreateTransaction_InvalidJSON(t *testing.T) {
	// Arrange
	mockRepo := new(MockTransactionRepository)
	controller := NewTransactionController(mockRepo, nil)

	router := setupTestRouter()
	router.POST("/transactions", controller.CreateTransaction)

	// Невалидный JSON
	jsonBody := []byte(`{"amount": "invalid"}`)

	// Act
	req, _ := http.NewRequest("POST", "/transactions", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assert
	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Contains(t, response, "error")
}

func TestCreateTransaction_RepositoryError(t *testing.T) {
	// Arrange
	mockRepo := new(MockTransactionRepository)
	controller := NewTransactionController(mockRepo, nil)

	router := setupTestRouter()
	router.POST("/transactions", controller.CreateTransaction)

	requestBody := map[string]interface{}{
		"amount":   150.0,
		"currency": "USD",
		"user_id":  123,
	}
	jsonBody, _ := json.Marshal(requestBody)

	// Симулируем ошибку репозитория
	mockRepo.On("Save", mock.AnythingOfType("*models.Transaction")).Return(nil, errors.New("database error"))

	// Act
	req, _ := http.NewRequest("POST", "/transactions", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assert
	assert.Equal(t, http.StatusInternalServerError, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "Failed to save transaction", response["error"])

	mockRepo.AssertExpectations(t)
}

func TestCreateTransaction_MissingFields(t *testing.T) {
	// Arrange
	mockRepo := new(MockTransactionRepository)
	controller := NewTransactionController(mockRepo, nil)

	router := setupTestRouter()
	router.POST("/transactions", controller.CreateTransaction)

	// Отсутствует обязательное поле amount (будет 0.0)
	requestBody := map[string]interface{}{
		"currency": "USD",
		"user_id":  123,
	}
	jsonBody, _ := json.Marshal(requestBody)

	expectedTransaction := &models.Transaction{
		ID:        uuid.New(),
		Amount:    0.0,
		Currency:  "USD",
		UserID:    123,
		Status:    models.StatusNew,
		Timestamp: time.Now(),
	}

	mockRepo.On("Save", mock.AnythingOfType("*models.Transaction")).Return(expectedTransaction, nil)

	// Act
	req, _ := http.NewRequest("POST", "/transactions", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assert - при отсутствии amount, он будет 0, что валидно для float64
	assert.Equal(t, http.StatusCreated, w.Code)

	var response models.Transaction
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, response.Amount)

	mockRepo.AssertExpectations(t)
}

func TestCreateTransaction_LargeAmount(t *testing.T) {
	// Arrange
	mockRepo := new(MockTransactionRepository)
	mockProducer := new(MockProducer)
	fraudService := services.NewFraudDetectionService(mockProducer, "test-topic")
	controller := NewTransactionController(mockRepo, fraudService)

	router := setupTestRouter()
	router.POST("/transactions", controller.CreateTransaction)

	// Большая сумма, которая должна быть помечена как мошенническая
	requestBody := map[string]interface{}{
		"amount":   15000.0,
		"currency": "USD",
		"user_id":  123,
	}
	jsonBody, _ := json.Marshal(requestBody)

	expectedTransaction := &models.Transaction{
		ID:        uuid.New(),
		Amount:    15000.0,
		Currency:  "USD",
		UserID:    123,
		Status:    models.StatusNew,
		Timestamp: time.Now(),
	}

	mockRepo.On("Save", mock.AnythingOfType("*models.Transaction")).Return(expectedTransaction, nil)
	mockProducer.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	// Act
	req, _ := http.NewRequest("POST", "/transactions", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Assert
	assert.Equal(t, http.StatusCreated, w.Code)

	var response models.Transaction
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, 15000.0, response.Amount)
	assert.Equal(t, models.StatusNew, response.Status) // Статус NEW, т.к. анализ идет асинхронно

	mockRepo.AssertExpectations(t)
}
