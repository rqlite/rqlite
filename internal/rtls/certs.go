package rtls

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"time"
)

// GenerateCACert generates a new CA certificate and returns the cert and key as PEM-encoded bytes.
func GenerateCACert(subject pkix.Name, validFor time.Duration, keySize int) ([]byte, []byte, error) {
	// generate a new private key
	key, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, nil, err
	}

	// generate a new certificate
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               subject,
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(validFor),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	// generate a new certificate
	cert, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}

	// encode the certificate and private key
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	return certPEM, keyPEM, nil
}

// GenerateCert generates a new x509 certificate and returns the cert and key as PEM-encoded bytes.
// The function should take in a subject, a validity period, and a key size. It should optionally
// take in a parent certificate and key. If a parent certificate and key are provided, the new
// certificate should be signed by the parent. If no parent certificate and key are provided,
// the new certificate should be self-signed.
func GenerateCert(subject pkix.Name, validFor time.Duration, keySize int, parent *x509.Certificate, parentKey any) ([]byte, []byte, error) {
	// generate a new private key
	key, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, nil, err
	}

	// generate a new certificate
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      subject,
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(validFor),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	signerCert := parent
	signerKey := parentKey
	if signerCert == nil {
		signerCert = &template
		signerKey = key
	}
	cert, err := x509.CreateCertificate(rand.Reader, &template, signerCert, &key.PublicKey, signerKey)
	if err != nil {
		return nil, nil, err
	}

	// encode the certificate and private key
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	return certPEM, keyPEM, nil
}

func GenerateCertIPSAN(subject pkix.Name, validFor time.Duration, keySize int, parent *x509.Certificate, parentKey any, san net.IP) ([]byte, []byte, error) {
	// generate a new private key
	key, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, nil, err
	}

	// generate a new certificate
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      subject,
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(validFor),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	// generate cert template for new certificate suitable for client verification

	// Add IP SAN to the certificate
	template.IPAddresses = append(template.IPAddresses, san)

	signerCert := parent
	signerKey := parentKey
	if signerCert == nil {
		signerCert = &template
		signerKey = key
	}
	cert, err := x509.CreateCertificate(rand.Reader, &template, signerCert, &key.PublicKey, signerKey)
	if err != nil {
		return nil, nil, err
	}

	// encode the certificate and private key
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	return certPEM, keyPEM, nil
}

// GenerateSelfSignedCert generates a new self-signed certificate and
// returns the cert and key as PEM-encoded bytes.
func GenerateSelfSignedCert(subject pkix.Name, validFor time.Duration, keySize int) ([]byte, []byte, error) {
	// generate a new private key
	key, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, nil, err
	}

	// generate a new certificate
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               subject,
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(validFor),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	// Add IP SAN to the certificate
	//template.IPAddresses = append(template.IPAddresses, net.ParseIP("127.0.0.1"))

	cert, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}

	// encode the certificate and private key
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	return certPEM, keyPEM, nil
}

// GenerateSelfSignedCertIPSAN generates a new self-signed certificate and
// returns the cert and key as PEM-encoded bytes.
func GenerateSelfSignedCertIPSAN(subject pkix.Name, validFor time.Duration, keySize int, san net.IP) ([]byte, []byte, error) {
	// generate a new private key
	key, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, nil, err
	}

	// generate a new certificate
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               subject,
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(validFor),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	template.IPAddresses = append(template.IPAddresses, san)

	cert, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}

	// encode the certificate and private key
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	return certPEM, keyPEM, nil
}
