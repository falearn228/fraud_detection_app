package repositories

import (
	"database/sql"
	"fraud-detection-app/internal/models"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type PostgresTransactionRepository struct {
	db *sql.DB
}

func NewPostgresTransactionRepository(db *sql.DB) TransactionRepository {
	return &PostgresTransactionRepository{
		db: db,
	}
}

func (r *PostgresTransactionRepository) Save(transaction *models.Transaction) (*models.Transaction, error) {
	query := `
		INSERT INTO transactions (id, amount, currency, user_id, status, timestamp)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id) DO UPDATE SET
			amount = EXCLUDED.amount,
			currency = EXCLUDED.currency,
			user_id = EXCLUDED.user_id,
			status = EXCLUDED.status,
			timestamp = EXCLUDED.timestamp
		RETURNING id, amount, currency, user_id, status, timestamp`

	var savedTransaction models.Transaction
	err := r.db.QueryRow(
		query,
		transaction.ID,
		transaction.Amount,
		transaction.Currency,
		transaction.UserID,
		string(transaction.Status),
		transaction.Timestamp,
	).Scan(
		&savedTransaction.ID,
		&savedTransaction.Amount,
		&savedTransaction.Currency,
		&savedTransaction.UserID,
		&savedTransaction.Status,
		&savedTransaction.Timestamp,
	)

	if err != nil {
		return nil, err
	}

	return &savedTransaction, nil
}

func (r *PostgresTransactionRepository) FindByID(id uuid.UUID) (*models.Transaction, error) {
	query := `SELECT id, amount, currency, user_id, status, timestamp FROM transactions WHERE id = $1`

	var transaction models.Transaction
	var status string
	err := r.db.QueryRow(query, id).Scan(
		&transaction.ID,
		&transaction.Amount,
		&transaction.Currency,
		&transaction.UserID,
		&status,
		&transaction.Timestamp,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	transaction.Status = models.Status(status)
	return &transaction, nil
}

func (r *PostgresTransactionRepository) FindAll() ([]*models.Transaction, error) {
	query := `SELECT id, amount, currency, user_id, status, timestamp FROM transactions ORDER BY timestamp DESC`

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var transactions []*models.Transaction
	for rows.Next() {
		var transaction models.Transaction
		var status string
		err := rows.Scan(
			&transaction.ID,
			&transaction.Amount,
			&transaction.Currency,
			&transaction.UserID,
			&status,
			&transaction.Timestamp,
		)
		if err != nil {
			return nil, err
		}

		transaction.Status = models.Status(status)
		transactions = append(transactions, &transaction)
	}

	return transactions, nil
}

func (r *PostgresTransactionRepository) InitSchema() error {
	query := `
		CREATE TABLE IF NOT EXISTS transactions (
			id UUID PRIMARY KEY,
			amount DECIMAL(15, 2) NOT NULL,
			currency VARCHAR(3) NOT NULL,
			user_id BIGINT NOT NULL,
			status VARCHAR(20) NOT NULL DEFAULT 'NEW',
			timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);
		CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp);
		CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);
	`

	_, err := r.db.Exec(query)
	return err
}