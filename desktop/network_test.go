package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBuildTransportProxy(t *testing.T) {
	app := NewApp()
	transport, err := app.buildTransport(NetworkSettings{ProxyURL: "http://127.0.0.1:8080"})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
	proxy, err := transport.Proxy(req)
	if err != nil {
		t.Fatal(err)
	}
	if proxy == nil || proxy.String() != "http://127.0.0.1:8080" {
		t.Fatalf("proxy = %v", proxy)
	}
}

func TestBuildTransportRejectsInvalidProxy(t *testing.T) {
	app := NewApp()
	if _, err := app.buildTransport(NetworkSettings{ProxyURL: "ftp://proxy"}); err == nil {
		t.Fatal("expected invalid proxy error")
	}
}

func writeTestCA(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "SRE Harness Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := pem.Encode(file, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestBuildTransportCustomCA(t *testing.T) {
	app := NewApp()
	transport, err := app.buildTransport(NetworkSettings{CACertPath: writeTestCA(t)})
	if err != nil {
		t.Fatal(err)
	}
	if transport.TLSClientConfig == nil || transport.TLSClientConfig.RootCAs == nil {
		t.Fatal("root CAs not configured")
	}
}

func TestBuildTransportMissingCA(t *testing.T) {
	app := NewApp()
	if _, err := app.buildTransport(NetworkSettings{CACertPath: "testdata/missing.pem"}); err == nil {
		t.Fatal("expected missing CA error")
	}
}
