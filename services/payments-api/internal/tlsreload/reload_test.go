package tlsreload

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReloadSwapsCertificate(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	caFile := filepath.Join(dir, "ca.crt")

	writePair(t, certFile, keyFile, 1)
	caPEM := readFile(t, certFile)
	mustWrite(t, caFile, caPEM)

	r, err := New(Options{
		CertFile:    certFile,
		KeyFile:     keyFile,
		CAFile:      caFile,
		RequireMTLS: true,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if got := servedSerial(t, r); got != 1 {
		t.Fatalf("initial serial = %d; want 1", got)
	}

	writePair(t, certFile, keyFile, 2)
	if err := r.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if got := servedSerial(t, r); got != 2 {
		t.Fatalf("serial after reload = %d; want 2", got)
	}
}

func TestInvalidReloadKeepsPreviousCertificate(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")

	writePair(t, certFile, keyFile, 3)

	r, err := New(Options{
		CertFile: certFile,
		KeyFile:  keyFile,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	mustWrite(t, certFile, []byte("not a certificate"))
	if err := r.Reload(); err == nil {
		t.Fatal("Reload succeeded on invalid material; want error")
	}
	if got := servedSerial(t, r); got != 3 {
		t.Fatalf("serial after failed reload = %d; want 3 (previous material)", got)
	}
}

func TestStartPicksUpReplacement(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")

	writePair(t, certFile, keyFile, 10)

	r, err := New(Options{
		CertFile: certFile,
		KeyFile:  keyFile,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Interval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Start(ctx)

	writePair(t, certFile, keyFile, 11)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if servedSerial(t, r) == 11 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("polling never picked up the replacement; serial = %d", servedSerial(t, r))
}

func servedSerial(t *testing.T, r *Reloader) int64 {
	t.Helper()
	conf, err := r.TLSConfig().GetConfigForClient(nil)
	if err != nil {
		t.Fatalf("GetConfigForClient: %v", err)
	}
	if len(conf.Certificates) != 1 {
		t.Fatalf("served %d certificates; want 1", len(conf.Certificates))
	}
	leaf, err := x509.ParseCertificate(conf.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatalf("parse served certificate: %v", err)
	}
	return leaf.SerialNumber.Int64()
}

func writePair(t *testing.T, certFile, keyFile string, serial int64) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: "resilientbank-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	mustWrite(t, certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	mustWrite(t, keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}
