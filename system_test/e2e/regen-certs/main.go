package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"time"
)

func main() {
	// For test fixtures, make validity effectively permanent.
	now := time.Unix(0, 0).UTC()
	notAfter := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

	// CA key.
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		log.Fatalf("generate CA key: %v", err)
	}

	caSerial, err := serialNumber()
	if err != nil {
		log.Fatalf("generate CA serial: %v", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: caSerial,
		Subject: pkix.Name{
			Country:            []string{"US"},
			Province:           []string{"CA"},
			Locality:           []string{"San Francisco"},
			Organization:       []string{"MyCA"},
			OrganizationalUnit: []string{"MyCAUnit"},
			CommonName:         "MyCA",
		},
		NotBefore:             now,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            1,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		log.Fatalf("create CA cert: %v", err)
	}

	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		log.Fatalf("parse CA cert: %v", err)
	}

	// Leaf key.
	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		log.Fatalf("generate leaf key: %v", err)
	}

	leafSerial, err := serialNumber()
	if err != nil {
		log.Fatalf("generate leaf serial: %v", err)
	}

	leafTemplate := &x509.Certificate{
		SerialNumber: leafSerial,
		Subject: pkix.Name{
			Country:            []string{"US"},
			Province:           []string{"CA"},
			Locality:           []string{"San Francisco"},
			Organization:       []string{"MyOrg"},
			OrganizationalUnit: []string{"MyUnit"},
			CommonName:         "example.com",
		},
		NotBefore:   now,
		NotAfter:    notAfter,
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: []string{
			"example.com",
		},
		IPAddresses: []net.IP{
			net.ParseIP("127.0.0.1"),
		},
	}

	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		log.Fatalf("create leaf cert: %v", err)
	}

	caCertPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caDER,
	})
	caKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(caKey),
	})
	leafCertPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: leafDER,
	})
	leafKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(leafKey),
	})

	// Print Go raw string literals for direct paste into test code.
	fmt.Println("x509cert = `")
	fmt.Print(string(caCertPEM))
	fmt.Println("`")
	fmt.Println()

	fmt.Println("x509key = `")
	fmt.Print(string(caKeyPEM))
	fmt.Println("`")
	fmt.Println()

	fmt.Println("caSignedCertExampleDotCom = `")
	fmt.Print(string(leafCertPEM))
	fmt.Println("`")
	fmt.Println()

	fmt.Println("caSignedKeyExampleDotCom = `")
	fmt.Print(string(leafKeyPEM))
	fmt.Println("`")
	fmt.Println()

	// Also write files, in case that is easier.
	mustWrite("ca.pem", caCertPEM)
	mustWrite("ca.key", caKeyPEM)
	mustWrite("example.com.pem", leafCertPEM)
	mustWrite("example.com.key", leafKeyPEM)

	fmt.Fprintln(os.Stderr, "wrote: ca.pem, ca.key, example.com.pem, example.com.key")
	fmt.Fprintln(os.Stderr, "valid from:", now.Format(time.RFC3339))
	fmt.Fprintln(os.Stderr, "valid until:", notAfter.Format(time.RFC3339))
}

func serialNumber() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, limit)
}

func mustWrite(path string, data []byte) {
	if err := os.WriteFile(path, data, 0o600); err != nil {
		log.Fatalf("write %s: %v", path, err)
	}
}
