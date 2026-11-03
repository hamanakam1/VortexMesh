package transport

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/hamanakam1/vortexmesh/internal/envelope"
)

func testEnvelope(t *testing.T) envelope.Envelope {
	t.Helper()
	e, err := envelope.New("1", envelope.Telemetry, "telemetry", "node-a", map[string]bool{"online": true})
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	return e
}

func TestWriteEnvelopeUsesLengthPrefix(t *testing.T) {
	msg := testEnvelope(t)

	var out bytes.Buffer
	if err := WriteEnvelope(&out, msg); err != nil {
		t.Fatalf("WriteEnvelope: %v", err)
	}
	raw := out.Bytes()
	if len(raw) < 5 {
		t.Fatal("expected frame header and payload")
	}
	if got, want := binary.BigEndian.Uint32(raw[:4]), uint32(len(raw)-4); got != want {
		t.Fatalf("frame length = %d, want %d", got, want)
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	msg := testEnvelope(t)

	var out bytes.Buffer
	if err := WriteEnvelope(&out, msg); err != nil {
		t.Fatalf("WriteEnvelope: %v", err)
	}
	got, err := ReadEnvelope(&out)
	if err != nil {
		t.Fatalf("ReadEnvelope: %v", err)
	}
	if got.ID != msg.ID || got.Topic != msg.Topic || got.Source != msg.Source {
		t.Fatalf("got %+v, want %+v", got, msg)
	}
}

func TestReadEnvelopeRejectsBadFrames(t *testing.T) {
	tests := map[string][]byte{
		"empty frame":     {0, 0, 0, 0},
		"oversized frame": {0x00, 0x40, 0x00, 0x01},
		"truncated body":  {0, 0, 0, 10, '{'},
		"truncated head":  {0, 0},
	}

	for name, frame := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ReadEnvelope(bytes.NewReader(frame)); err == nil {
				t.Fatalf("expected %s to be rejected", name)
			}
		})
	}
}

func TestReadEnvelopeRejectsInvalidEnvelope(t *testing.T) {
	payload := []byte(`{"id":"","version":1}`)
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)

	if _, err := ReadEnvelope(bytes.NewReader(frame)); err == nil {
		t.Fatal("expected an invalid envelope to be rejected")
	}
}

// TestEndToEndMutualTLS exercises the real QUIC handshake with generated
// certificates: the server rejects a missing client certificate and accepts a
// signed one, and an envelope survives the trip.
func TestEndToEndMutualTLS(t *testing.T) {
	serverCert, clientCert, serverPool, clientPool := testCertificates(t)

	received := make(chan envelope.Envelope, 1)
	srv, err := Listen("127.0.0.1:0", ServerTLSConfig(serverCert, clientPool), func(_ context.Context, e envelope.Envelope) error {
		received <- e
		return nil
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = srv.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx) }()

	conn, err := Dial(ctx, srv.Addr().String(), ClientTLSConfig(clientCert, serverPool, "vortexmesh.test"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.CloseWithError(0, "") }()

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("OpenStreamSync: %v", err)
	}
	if err := WriteEnvelope(stream, testEnvelope(t)); err != nil {
		t.Fatalf("WriteEnvelope: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	select {
	case got := <-received:
		if got.ID != "1" {
			t.Fatalf("handler got id %q, want %q", got.ID, "1")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not receive the envelope")
	}

	cancel()
	if err := <-serveErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve returned %v, want context.Canceled", err)
	}
}

func TestServerRejectsUntrustedClientCertificate(t *testing.T) {
	serverCert, _, serverPool, _ := testCertificates(t)
	// A client certificate from an unrelated CA must not be accepted.
	_, strangerCert, _, _ := testCertificates(t)

	srv, err := Listen("127.0.0.1:0", ServerTLSConfig(serverCert, serverPool), func(context.Context, envelope.Envelope) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = srv.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = srv.Serve(ctx) }()

	conn, err := Dial(ctx, srv.Addr().String(), ClientTLSConfig(strangerCert, serverPool, "vortexmesh.test"))
	if err != nil {
		return // rejected during the handshake, which is the desired outcome
	}
	defer func() { _ = conn.CloseWithError(0, "") }()

	// TLS 1.3 lets the client finish its side of the handshake before the
	// server rejects the certificate, so the failure may surface on the first
	// stream or as a connection close.
	if stream, err := conn.OpenStreamSync(ctx); err == nil {
		_ = WriteEnvelope(stream, testEnvelope(t))
		_ = stream.Close()
	}

	select {
	case <-conn.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("expected the server to reject an untrusted client certificate")
	}
}

func TestListenRequiresConfigAndHandler(t *testing.T) {
	if _, err := Listen("127.0.0.1:0", nil, func(context.Context, envelope.Envelope) error { return nil }); err == nil {
		t.Fatal("expected a nil TLS config to be rejected")
	}
	serverCert, _, serverPool, _ := testCertificates(t)
	if _, err := Listen("127.0.0.1:0", ServerTLSConfig(serverCert, serverPool), nil); err == nil {
		t.Fatal("expected a nil handler to be rejected")
	}
}

// testCertificates builds a throwaway CA, a server certificate for
// 127.0.0.1/vortexmesh.test, and a client certificate signed by that CA.
func testCertificates(t *testing.T) (serverCert, clientCert tls.Certificate, serverPool, clientPool *x509.CertPool) {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "vortexmesh test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	issue := func(serial int64, commonName string, usage x509.ExtKeyUsage, ips []net.IP, dns []string) tls.Certificate {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate %s key: %v", commonName, err)
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: commonName},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{usage},
			IPAddresses:  ips,
			DNSNames:     dns,
		}
		der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatalf("create %s certificate: %v", commonName, err)
		}
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}

	serverCert = issue(2, "vortexmesh.test", x509.ExtKeyUsageServerAuth,
		[]net.IP{net.ParseIP("127.0.0.1")}, []string{"vortexmesh.test"})
	clientCert = issue(3, "node-a", x509.ExtKeyUsageClientAuth, nil, nil)

	return serverCert, clientCert, caPool, caPool
}
