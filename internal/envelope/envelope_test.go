package envelope

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewAndValidate(t *testing.T) {
	e, err := New("m-1", Telemetry, "agents/a1", "a1", map[string]any{"online": true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if e.Version != Version {
		t.Fatalf("version = %d, want %d", e.Version, Version)
	}
	if e.CreatedAt.Location() != time.UTC {
		t.Fatalf("created_at = %v, want UTC", e.CreatedAt.Location())
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestRejectsMissingFields(t *testing.T) {
	if err := (Envelope{Version: 1, Kind: Telemetry}).Validate(); err == nil {
		t.Fatal("expected validation error for missing fields")
	}
}

func TestValidateRejectsBadValues(t *testing.T) {
	base := Envelope{
		ID:        "m-1",
		Version:   Version,
		Kind:      Telemetry,
		Topic:     "t",
		Source:    "s",
		CreatedAt: time.Now().UTC(),
		Payload:   json.RawMessage(`{"ok":true}`),
	}

	tests := map[string]func(*Envelope){
		"unknown kind":       func(e *Envelope) { e.Kind = "nope" },
		"unknown version":    func(e *Envelope) { e.Version = 2 },
		"zero created_at":    func(e *Envelope) { e.CreatedAt = time.Time{} },
		"invalid payload":    func(e *Envelope) { e.Payload = json.RawMessage(`{`) },
		"empty payload":      func(e *Envelope) { e.Payload = nil },
		"missing source":     func(e *Envelope) { e.Source = "" },
		"missing topic":      func(e *Envelope) { e.Topic = "" },
		"missing message id": func(e *Envelope) { e.ID = "" },
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			e := base
			mutate(&e)
			if err := e.Validate(); err == nil {
				t.Fatalf("expected validation error for %s", name)
			}
		})
	}
}

func TestDecodeRoundTrip(t *testing.T) {
	original, err := New("m-2", TaskRequest, "tasks", "a1", map[string]string{"action": "sync"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := original.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.ID != original.ID || decoded.Kind != original.Kind || decoded.Topic != original.Topic {
		t.Fatalf("decoded = %+v, want %+v", decoded, original)
	}
}

func TestDecodeRejectsTrailingData(t *testing.T) {
	valid, err := New("m-3", StateSync, "sync", "a1", map[string]int{"v": 1})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := valid.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(append(raw, []byte(`{"extra":true}`)...)); err == nil {
		t.Fatal("expected trailing data to be rejected")
	}
}

func TestMarshalRejectsInvalidEnvelope(t *testing.T) {
	if _, err := (Envelope{ID: "x"}).MarshalJSON(); err == nil {
		t.Fatal("expected marshal to refuse an invalid envelope")
	}
}
