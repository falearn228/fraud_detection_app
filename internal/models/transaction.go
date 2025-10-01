package models

import (
	"time"

	"github.com/google/uuid"
)

// Transaction представляет сущность транзакции
type Transaction struct {
	ID        uuid.UUID `json:"id"`
	Amount    float64   `json:"amount"`
	Currency  string    `json:"currency"`
	UserID    int64     `json:"user_id"`
	Status    Status    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

// Status представляет статус транзакции
type Status string

const (
	StatusNew        Status = "NEW"
	StatusSuspicious Status = "SUSPICIOUS"
	StatusConfirmed  Status = "CONFIRMED"
)
