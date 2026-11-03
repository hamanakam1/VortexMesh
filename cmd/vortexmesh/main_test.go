package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunPrintsBannerEnvelopeAndMetrics(t *testing.T) {
	var out bytes.Buffer

	if err := run(context.Background(), &out, "telemetry"); err != nil {
		t.Fatalf("run: %v", err)
	}

	got := out.String()
	for _, want := range []string{"edge-to-cloud event mesh", `"topic": "telemetry"`, `"status": "online"`, "accepted=1 delivered=1 dropped=0"} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}

func TestRunHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var out bytes.Buffer
	if err := run(ctx, &out, "telemetry"); err == nil {
		t.Fatal("expected a cancelled context to abort the demo")
	}
}
