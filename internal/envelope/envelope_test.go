package envelope

import "testing"

func TestNewAndValidate(t *testing.T) {
	e, err := New("m-1", Telemetry, "agents/a1", "a1", map[string]any{"online": true})
	if err != nil { t.Fatal(err) }
	if err := e.Validate(); err != nil { t.Fatal(err) }
}

func TestRejectsMissingFields(t *testing.T) {
	if err := (Envelope{Version: 1, Kind: Telemetry}).Validate(); err == nil { t.Fatal("expected validation error") }
}
