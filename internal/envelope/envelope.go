// Package envelope defines the versioned message format that every VortexMesh
// node exchanges. A single Envelope type travels across the broker, the
// write-ahead log, and the QUIC transport, so validation lives here and is
// reused by each boundary instead of being duplicated.
package envelope

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Version is the only envelope schema version this build understands.
const Version = 1

// Kind identifies the intent of a message.
type Kind string

const (
	// TaskRequest asks a peer to perform work.
	TaskRequest Kind = "task.request"
	// Telemetry is a periodic health or status report.
	Telemetry Kind = "telemetry.heartbeat"
	// StateSync carries a reconciliation payload after a partition heals.
	StateSync Kind = "state.sync"
)

// Valid reports whether k is a known kind.
func (k Kind) Valid() bool {
	switch k {
	case TaskRequest, Telemetry, StateSync:
		return true
	default:
		return false
	}
}

// Envelope is the unit of transfer. Payload is opaque JSON so the mesh never
// needs to understand application schemas.
type Envelope struct {
	ID        string          `json:"id"`
	Version   int             `json:"version"`
	Kind      Kind            `json:"kind"`
	Topic     string          `json:"topic"`
	Source    string          `json:"source"`
	CreatedAt time.Time       `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

// New builds an envelope with the current schema version and a UTC timestamp,
// then validates it so callers cannot construct an invalid message.
func New(id string, kind Kind, topic, source string, payload any) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal payload: %w", err)
	}
	e := Envelope{
		ID:        id,
		Version:   Version,
		Kind:      kind,
		Topic:     topic,
		Source:    source,
		CreatedAt: time.Now().UTC(),
		Payload:   raw,
	}
	if err := e.Validate(); err != nil {
		return Envelope{}, err
	}
	return e, nil
}

// Validate enforces the invariants every boundary relies on: identity fields
// are present, the schema version is known, the kind is recognised, and the
// payload is valid JSON.
func (e Envelope) Validate() error {
	if e.ID == "" || e.Topic == "" || e.Source == "" {
		return errors.New("id, topic, and source are required")
	}
	if e.Version != Version {
		return fmt.Errorf("unsupported envelope version %d", e.Version)
	}
	if !e.Kind.Valid() {
		return fmt.Errorf("unsupported kind %q", e.Kind)
	}
	if e.CreatedAt.IsZero() {
		return errors.New("created_at is required")
	}
	if len(e.Payload) == 0 || !json.Valid(e.Payload) {
		return errors.New("payload must be valid JSON")
	}
	return nil
}

// Decode parses and validates an envelope received from an untrusted source.
func Decode(data []byte) (Envelope, error) {
	var e Envelope
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&e); err != nil {
		return Envelope{}, fmt.Errorf("decode envelope: %w", err)
	}
	if dec.More() {
		return Envelope{}, errors.New("decode envelope: trailing data after object")
	}
	if err := e.Validate(); err != nil {
		return Envelope{}, err
	}
	return e, nil
}

// MarshalJSON refuses to serialise an invalid envelope, so a bug cannot leak a
// malformed message onto the wire or into the write-ahead log.
func (e Envelope) MarshalJSON() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	type alias Envelope
	return json.Marshal(alias(e))
}
