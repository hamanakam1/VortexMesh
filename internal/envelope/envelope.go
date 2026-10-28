package envelope

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Kind string

const (
	TaskRequest Kind = "task.request"
	Telemetry  Kind = "telemetry.heartbeat"
	StateSync  Kind = "state.sync"
)

type Envelope struct {
	ID        string          `json:"id"`
	Version   int             `json:"version"`
	Kind      Kind            `json:"kind"`
	Topic     string          `json:"topic"`
	Source    string          `json:"source"`
	CreatedAt time.Time       `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

func New(id string, kind Kind, topic, source string, payload any) (Envelope, error) {
	b, err := json.Marshal(payload)
	if err != nil { return Envelope{}, fmt.Errorf("marshal payload: %w", err) }
	e := Envelope{ID: id, Version: 1, Kind: kind, Topic: topic, Source: source, CreatedAt: time.Now().UTC(), Payload: b}
	return e, e.Validate()
}

func (e Envelope) Validate() error {
	if e.ID == "" || e.Topic == "" || e.Source == "" { return errors.New("id, topic, and source are required") }
	if e.Version != 1 { return fmt.Errorf("unsupported envelope version %d", e.Version) }
	if e.Kind != TaskRequest && e.Kind != Telemetry && e.Kind != StateSync { return fmt.Errorf("unsupported kind %q", e.Kind) }
	if e.CreatedAt.IsZero() || len(e.Payload) == 0 || !json.Valid(e.Payload) { return errors.New("created_at and valid JSON payload are required") }
	return nil
}

func (e Envelope) MarshalJSON() ([]byte, error) { if err := e.Validate(); err != nil { return nil, err }; type alias Envelope; return json.Marshal(alias(e)) }
