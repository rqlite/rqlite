package x509

import (
	"os"
)

// CertExampleDotComFile returns the path to a temporary file, in directory dir, containing
// a cert for example.com. It is up to the caller to remove the file when finished. If
// dir is the empty string then the default directory for temporary files is used.
func CertExampleDotComFile(dir string) string {
	return mustWriteToFile(dir, certExampleDotCom)
}

// KeyExampleDotComFile returns the path to a temporary file, in directory dir, containing
// a key for example.com. It is up to the caller to remove the file when finished. If
// dir is the empty string then the default directory for temporary files is used.
func KeyExampleDotComFile(dir string) string {
	return mustWriteToFile(dir, keyExampleDotCom)
}

func mustWriteToFile(dir, content string) string {
	b := []byte(content)

	path := mustTempFile(dir)
	if err := os.WriteFile(path, b, 0600); err != nil {
		panic(err.Error())
	}
	return path
}

// mustTempFile returns a path to a temporary file in directory dir. It is up to the
// caller to remove the file once it is no longer needed. If dir is the empty
// string, then the default directory for temporary files is used.
func mustTempFile(dir string) string {
	tmpfile, err := os.CreateTemp(dir, "rqlite-tls-test")
	if err != nil {
		panic(err.Error())
	}
	tmpfile.Close()
	return tmpfile.Name()
}

const certExampleDotCom = `-----BEGIN CERTIFICATE-----
MIID3zCCAsegAwIBAgIUaswvhndvKfJOZL8oEtNZi3LHvgIwDQYJKoZIhvcNAQEL
BQAwczELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAkNBMRYwFAYDVQQHDA1TYW4gRnJh
bmNpc2NvMRMwEQYDVQQKDApNeSBDb21wYW55MRQwEgYDVQQLDAtNeSBEaXZpc2lv
bjEUMBIGA1UEAwwLZXhhbXBsZS5jb20wHhcNMjQxMjIwMTQzOTE2WhcNMzQxMjE4
MTQzOTE2WjBzMQswCQYDVQQGEwJVUzELMAkGA1UECAwCQ0ExFjAUBgNVBAcMDVNh
biBGcmFuY2lzY28xEzARBgNVBAoMCk15IENvbXBhbnkxFDASBgNVBAsMC015IERp
dmlzaW9uMRQwEgYDVQQDDAtleGFtcGxlLmNvbTCCASIwDQYJKoZIhvcNAQEBBQAD
ggEPADCCAQoCggEBAKtdyjBHUjt25vsxm43hEPfLgrh3L9pGvgAkaQJdp81AVR/k
PntJdLqrOacMjXv6zo5Os5iDl7+cce6/aPvsKlpgFd+73aj1dTnkHSo086v5Ojkg
+K8r568Csx0bkVimuaYE9TafDCE7VjKi5vE9fspahYsjjeHcpI+3XzFG4Hhi15U6
oqc8dwG+uN18UB5HIRABIbSgeC63wHdVilHX5X2lRSIF05LK7Zoswbft/QduJAjg
bLLxDDymyjfOzb53empwYsl+31ag10cw2ejjBEb/bIuTskp+Sfz0uHyv2ZMx5aC9
87Un17d+v5EzEn4m79XAaiXyQ1+zPCoTXIcTBwMCAwEAAaNrMGkwHQYDVR0OBBYE
FFKaJfCmeG0yxFfJGDYc0xho0K3HMB8GA1UdIwQYMBaAFFKaJfCmeG0yxFfJGDYc
0xho0K3HMA8GA1UdEwEB/wQFMAMBAf8wFgYDVR0RBA8wDYILZXhhbXBsZS5jb20w
DQYJKoZIhvcNAQELBQADggEBAKCRg6HzXVWmy2rN6lbnbonP/oZsVubxZ+cVcucp
y8F+johEygI2TE2NkCYF+7PBfFvkfFKWfbRNe+Akmg4WuNkmWTWETbmyCwVZfre3
vaZn7XalAYs7xFhaVa0bmJv4Mu46IdEgjh652FGYC3rWxMyyVpe7QrjLEHG/GmLR
QHirO2G8PTAwUiPRjda/N/o1FWNB8egQsNcMeCmLNJKMcA1Jx28/RttDIRY+IOko
1K0Ty8WLdR9ZQBL/z6XOah15BAYdu9WC0kiScI6Vd4bz0KlKJ6RlW/iHHeX4y2bP
OJxxJzcmI18JZoPRla/pfdEU+0kEFtSzckQzOihnja5W1b0=
-----END CERTIFICATE-----`

const keyExampleDotCom = `-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCrXcowR1I7dub7
MZuN4RD3y4K4dy/aRr4AJGkCXafNQFUf5D57SXS6qzmnDI17+s6OTrOYg5e/nHHu
v2j77CpaYBXfu92o9XU55B0qNPOr+To5IPivK+evArMdG5FYprmmBPU2nwwhO1Yy
oubxPX7KWoWLI43h3KSPt18xRuB4YteVOqKnPHcBvrjdfFAeRyEQASG0oHgut8B3
VYpR1+V9pUUiBdOSyu2aLMG37f0HbiQI4Gyy8Qw8pso3zs2+d3pqcGLJft9WoNdH
MNno4wRG/2yLk7JKfkn89Lh8r9mTMeWgvfO1J9e3fr+RMxJ+Ju/VwGol8kNfszwq
E1yHEwcDAgMBAAECggEAIHx/P/fFVxCLaUNWQedQ2Cj0dxPhNngCvPQqFBC9JuPz
P0B9t9GNF3YuT8TJbZ92WPQCh/8qXWDDGIeg12FGBiwA+ZEbzFP8DSV5ge7X4to0
d3AQtOSV30+btQDs5Ol2eEqoUdqE6ifdh2vqbAFEcAgJGP98fjzd1YDlwum01B1Z
ZrHng/8IReYHEjFPhv3ul95Olh3aGIHvICUMOyVjRmGlW2dCH80xhaC6u52Qewbr
5P7tCZY3KINq5RXzS4w7GD1LxiFLmT4syyOoHoEfog0s/6rDIb6lzold4O4EE8Ca
lPSHpfp8XgBJGJLg9Rly/O5ulV40eYU/95iJXfK/wQKBgQDTFOJHlRlIYvGGhiSe
PSQkTEYyVq/WM6ouHd/X4zpGPLxvoCu+Pa3pxQQ6KhUgY+rSwrUj3vfxTycL5oGO
NlW4G31s51na6u9JgT6PjJJNpd7SNDe/bE27hdlp5EucEwgez5WmakqRIyOgUTTU
GLi54S4JaxmF5JKSOTISuVKS1wKBgQDP1VXrhBLGVqK603X9iSnLr9kiDBY2X1vh
GejU4VADkJd9YJ+dgvQbWNIlDcIxr2TYCMomh7+5iDuX5iMKa/QgLSX6l9xzeNgq
1z7f/iotvrMqzt/t/qB+XmW5h9O8Olal5hxWowLPt92yG1D2hoOjcahk7/3036VU
yFGGqWbTtQKBgQCr3KUgg1VCisz8KtxFuqJiq3e8JLimPwqg4dIPhQM9jNYgTNlQ
3Adt/uuiMAFjjfb0A3RY8IIQB9JS7isuDy9b54YH1ZZjmEWcI488ccftUNBLdhit
0xC3ThPnE+o7+YLzEzFVfdWYtnObZIMO7dH6Bk3lfT4atjBgvhD3Dc0doQKBgQCV
iIgOMPOVMqZYx2aAaZzhyX7viUS+EDQ6LHsiF46LUhA9DDUrjhn2RxzDvjd3qhtj
eEeCG2+tnMBL1TBnAi8eq31E5NifWFYn1MCPRv6v9SJR7ZCeWvK8mUyVhY2pQ0wu
hvbiutx9+WuQBylkhnnWMiOXpDjTY8o/yvUlb2LldQKBgECegCiMv5YzWMY0Ok3Y
1U0bPBMdTOCmZnBkeC6paDMj175gtDSt+KoAT7yo7zIy3Az2EcvNX1ppeexg1E6I
SKvTKlHv2kAfV3cw5Ydc4Sumt5t47FTsu9OVXqNUc9VB16ZWiMva8m9Wo0zCKyNW
zSC1WQcaqiQ2dd2dEOKZbLe0
-----END PRIVATE KEY-----`
