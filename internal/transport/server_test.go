package transport

import (
	"bytes"
	"testing"
	"github.com/hamanakam1/vortexmesh/internal/envelope"
)

func TestWriteEnvelopeUsesLengthPrefix(t *testing.T) {
	msg, err := envelope.New("1", envelope.Telemetry, "telemetry", "node-a", map[string]bool{"online": true})
	if err != nil { t.Fatal(err) }
	var out bytes.Buffer
	if err := WriteEnvelope(&out, msg); err != nil { t.Fatal(err) }
	if out.Len() < 5 { t.Fatal("expected frame header and payload") }
}
