package jobs

import (
	"encoding/json"
	"time"
)

// Status espelha o CHECK da coluna jobs.status.
type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

// Job é uma linha da tabela jobs.
// Ponteiros representam colunas que podem ser NULL.
type Job struct {
	ID             int64
	Payload        json.RawMessage
	Status         Status
	Attempts       int
	LeaseUntil     *time.Time
	IdempotencyKey *string
	AvailableAt    time.Time
	MaxAttempts    int
	LastError      *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// TaskID lê o campo "id" do payload JSON, no formato {"id": N}.
// O segundo retorno é false quando o JSON não tem esse número.
func (j Job) TaskID() (int, bool) {
	var body struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(j.Payload, &body); err != nil || body.ID <= 0 {
		return 0, false
	}
	return body.ID, true
}

// PayloadFromTaskID monta o JSON gravado pelo POST /jobs?id=N.
func PayloadFromTaskID(id int) (json.RawMessage, error) {
	return json.Marshal(map[string]int{"id": id})
}
