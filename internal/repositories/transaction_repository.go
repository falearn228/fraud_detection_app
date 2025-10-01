package repositories

import (
	"fraud-detection-app/internal/models"

	"github.com/google/uuid"
)

// TransactionRepository определяет интерфейс для работы с транзакциями
type TransactionRepository interface {
	Save(transaction *models.Transaction) (*models.Transaction, error)
	FindByID(id uuid.UUID) (*models.Transaction, error)
	FindAll() ([]*models.Transaction, error)
}
