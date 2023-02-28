package x509

import (
	"io/ioutil"
)

// CertFile returns the path to a temporary file, in directory dir, containing a cert.
// It is up to the caller to remove the file when finished. If dir is the empty string
// then the default directory for temporary files is used.
func CertFile(dir string) string {
	return mustWriteToFile(dir, cert)
}

// KeyFile returns the path to a temporary file, in directory dir, containing a key.
// It is up to the caller to remove the file when finished.If dir is the empty string
// then the default directory for temporary files is used.
func KeyFile(dir string) string {
	return mustWriteToFile(dir, key)
}

// CAPrivateKeyFile returns the path to a temporary file, in directory dir, containing a key.
// It is up to the caller to remove the file when finished.If dir is the empty string
// then the default directory for temporary files is used.
func CAPrivateKeyFile(dir string) string {
	return mustWriteToFile(dir, caPrivateKey)
}

// CACertFile returns the path to a temporary file, in directory dir, containing the CA
// cert. It is up to the caller to remove the file when finished.If dir is the empty string
// then the default directory for temporary files is used.
func CACertFile(dir string) string {
	return mustWriteToFile(dir, caCert)
}

// ServerPrivateKeyFile returns the path to a temporary file, in directory dir, containing a key.
// It is up to the caller to remove the file when finished.If dir is the empty string
// then the default directory for temporary files is used.
func ServerPrivateKeyFile(dir string) string {
	return mustWriteToFile(dir, serverPrivateKey)
}

// ServerCASignedCertFile returns the path to a temporary file, in directory dir, containing
// a CA-signed cert. It is up to the caller to remove the file when finished.If dir is the
// empty string then the default directory for temporary files is used.
func ServerCASignedCertFile(dir string) string {
	return mustWriteToFile(dir, serverCASignedCert)
}

// ClientPrivateKeyFile returns the path to a temporary file, in directory dir, containing a key.
// It is up to the caller to remove the file when finished.If dir is the empty string
// then the default directory for temporary files is used.
func ClientPrivateKeyFile(dir string) string {
	return mustWriteToFile(dir, clientPrivateKey)
}

// ClientCASignedCertFile returns the path to a temporary file, in directory dir, containing
// a CA-signed cert. It is up to the caller to remove the file when finished.If dir is the
// empty string then the default directory for temporary files is used.
func ClientCASignedCertFile(dir string) string {
	return mustWriteToFile(dir, clientCASignedCert)
}

func mustWriteToFile(dir, content string) string {
	b := []byte(content)

	path := mustTempFile(dir)
	if err := ioutil.WriteFile(path, b, 0600); err != nil {
		panic(err.Error())
	}
	return path
}

// mustTempFile returns a path to a temporary file in directory dir. It is up to the
// caller to remove the file once it is no longer needed. If dir is the empty
// string, then the default directory for temporary files is used.
func mustTempFile(dir string) string {
	tmpfile, err := ioutil.TempFile(dir, "rqlite-tls-test")
	if err != nil {
		panic(err.Error())
	}
	tmpfile.Close()
	return tmpfile.Name()
}

const cert = `-----BEGIN CERTIFICATE-----
MIIFXTCCA0WgAwIBAgIJALrA6P0W35jRMA0GCSqGSIb3DQEBCwUAMEUxCzAJBgNV
BAYTAlVTMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQwHhcNMTcwNjEwMjIwMDM1WhcNMTgwNjEwMjIwMDM1WjBF
MQswCQYDVQQGEwJVUzETMBEGA1UECAwKU29tZS1TdGF0ZTEhMB8GA1UECgwYSW50
ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIIC
CgKCAgEA2cxg1IcP1gDQezLJm9MDkEEHqOZEAn1iatoIHUoIlfu36Sripn4yoTxM
1pmOT37CFoaiRfj0biEbjrgfi0QXk9z4E7Vy0XGF6XB5KofOneqnUuSgnOnEkL0p
gQ3itCr/FLkvuT8/zYKL+PXsMnfHGORgJmHlu1/4rY6Z/dayaf4fUFlKRRziEVUn
3EMd/hHFHThXimWd3mtxE1YnpKimnFLmIYjXrK22QUZJ2MYVcRklJYaXhIJgHW2s
oe+ZRhFHxcYoY3znRFZXYkoCXETcExCmo7czLoN4/F92zFDEGbAMbwC/7Zo9AxQg
30Q4iCrLfwAx+M/0A2dRbSTqGReBeBVfEBWopfz7zV3W7kI+s5K2AIFi+1hbmJ6a
mKomv3f4z6Ml+yOqrq4KtrDSxnSf6Vh7EHsws6uyMG7Y6rLpPm1sLDiffPABlAti
/YlVT+3vlg86h7Vlw68CcNSclgyfFW+i1e5a+EV7WB0VmIQXzSkhA86b9aD8qWdL
N4H8sRlSZ3XfIil4u93QDC/NzJl22wRsN7926xR4DgbCesEsc361KYE8fBSx61fa
6EyvlQoI2I4r1aWCSHq7YGfV6guBZekR0BeaIsoNwfZDZrboL0sOrHGxiEfzYdVC
pAxjdG13zuPo+634fUfewBAq695kVYcy3aBt2wOkLyQGLu0CHHsCAwEAAaNQME4w
HQYDVR0OBBYEFAYLLJUqmUdXCNYTQIWX1ICBKGvWMB8GA1UdIwQYMBaAFAYLLJUq
mUdXCNYTQIWX1ICBKGvWMAwGA1UdEwQFMAMBAf8wDQYJKoZIhvcNAQELBQADggIB
AGnvTPevCooo3xO8U/lq2YNo3cFxYnkurBwAn5pwzJrtPgogwezBltEp52+n39TY
5hSa//pfKdQz2GrQ9YvX1vB8gWkNLxBe6g2ksn0DsGTApC/te1p4M+yTKhogtE7a
qYmZBSEI46URe0JLYNirzdTu5dri7DzxFc7E/XlQ0riuMyHNqOP0JXKhxKN1dYOu
NEPxekq2Z2phoo1ul8hBXsz4IRwVeQOAtpRnfrKjxogOI1teP/RSikTsSLvFHxqo
UHVzwBexQs9isBlBUcmuKksxoGugqqSkGQRE+dSs5RSeEPLexMgACfFmKfpS+Vn4
ikb2ETQ3i76+JgMoDHKwb4u9xIyKTUToIsx5dUO+o7paPfyqRE6WbO4H+suM4VCd
VhNbG9qv02Fl8vdYAc/A6tVyV8b4fMbSsGEQnBlvKuOXf/uxAIcz11WUQ4gy/0/e
kHbMqGuBFPkg5nww3dBxkrBbtKq/1yrnQUjpBvjYtyUvoKrLSbQSGj586i52r4hF
+bqGPTxmk6hU4JZN+0wvkbVWLZBTRVNKs8Sb6fRWTd2Zd/o7a7QFhbnnAhv8bgyb
4472yLaXTL/siml+LlSrNGeZEsAaCVH4ETp+HzjpAMAyhhFGqCixG0e9BRPGV936
H/8+SUQK5KxnwDz3hqrAVJyimrvNlSaP1eZ5P8WXuvBl
-----END CERTIFICATE-----`

const key = `-----BEGIN PRIVATE KEY-----
MIIJQQIBADANBgkqhkiG9w0BAQEFAASCCSswggknAgEAAoICAQDZzGDUhw/WANB7
Msmb0wOQQQeo5kQCfWJq2ggdSgiV+7fpKuKmfjKhPEzWmY5PfsIWhqJF+PRuIRuO
uB+LRBeT3PgTtXLRcYXpcHkqh86d6qdS5KCc6cSQvSmBDeK0Kv8UuS+5Pz/Ngov4
9ewyd8cY5GAmYeW7X/itjpn91rJp/h9QWUpFHOIRVSfcQx3+EcUdOFeKZZ3ea3ET
ViekqKacUuYhiNesrbZBRknYxhVxGSUlhpeEgmAdbayh75lGEUfFxihjfOdEVldi
SgJcRNwTEKajtzMug3j8X3bMUMQZsAxvAL/tmj0DFCDfRDiIKst/ADH4z/QDZ1Ft
JOoZF4F4FV8QFail/PvNXdbuQj6zkrYAgWL7WFuYnpqYqia/d/jPoyX7I6qurgq2
sNLGdJ/pWHsQezCzq7Iwbtjqsuk+bWwsOJ988AGUC2L9iVVP7e+WDzqHtWXDrwJw
1JyWDJ8Vb6LV7lr4RXtYHRWYhBfNKSEDzpv1oPypZ0s3gfyxGVJndd8iKXi73dAM
L83MmXbbBGw3v3brFHgOBsJ6wSxzfrUpgTx8FLHrV9roTK+VCgjYjivVpYJIertg
Z9XqC4Fl6RHQF5oiyg3B9kNmtugvSw6scbGIR/Nh1UKkDGN0bXfO4+j7rfh9R97A
ECrr3mRVhzLdoG3bA6QvJAYu7QIcewIDAQABAoICAEKMgXXPAxa3zvwl65ZyZp9Y
T3fbTCKan0zY7CvO6EqzzGExmmmXG+9KVowoBWTi7XkmkETjKgTQlvQH7JOILdAf
b6nOApRepLVMialmL8ru3Uul0jG/+DDlq93kGUZF8QUrBJsM6XjpD831jsNo9+vy
NDLmLOURERIvBXybco6SeIz7i4cMqUL0iyZxV6O/WERyZ8VBAXjpyXZIF/rnEWmo
purOPmBj/9F4Ia5b8EdLkJ8jvf5eO/IiBeLBLEtNkmmq/8JOcvfdjfvZc1kwLTKi
HtjdbIUk5P3wSYNqllDnCxWL3BlEzKm5J8YwuTlaIi3fKGXHXN8BXc8EvYcHOKah
K89HIuexjQyQ0JAWKIIJTZs8jVvTMTjgYnEAB+sLfehBBOKmRdmYij28kIo18blx
tsx1HjdfImDd0QloofRW1Srp6FhcgDK0qfWXze/Vm6IfF40oTVE3fS/RgYzx0SSM
2pc6hTXOnrw1r/UBPyNkJ1D/4UK4m0x91BvTSi6MsThWnhicoaTZl1GP4Qpeo9+4
9Z7t0Yalm0PA55aiHZsm9S8OroasVun2QnDxfUC44PIov7nhqifGVcKA8hIDgSNT
WP8amq9cNjft5xQnP/y70fbioPPiwau2+Q0SXVn/BYxjqZrNp6OfbWSi2IRO1NOD
QZDo2rtnL1RrDdBmtDShAoIBAQD2MTJT6HNacu8+u7DDtbMdKvy7rXVblGbUouh9
cLWX7/zGVcNzB5GSVki3J7J+Kdrs4H45/1JR1YvWtWd93F/xKXmCGkUQCsturtRn
IX93by3zuWNdLv4giP2pk97wNYaaJWZmo87nXKV6BbL//eEl+Ospg5lGrLCsj+Mk
9V8oBBxsxqgVZYVyoevLDAuwUw3Cb52PhnEaLrv30ljGFHpsYb9lFlMs8vRosVWy
i3/T5ASfdnMXKQ1gxN/aPtN6yrFVpXe+S/A5JBzAQfrjiZk4SzNvE7R0eze1YLfO
IulTvlqpk3HVQEpgfq8D3l1x/zqsh0SpCH3VkV5sQQx262iRAoIBAQDieZsWv9eO
QzF5nZmh3NL53LkpqONRBjF5b9phppgxw2jiS8D2eEn2XWExmEaK/JppmzvfxSG4
cPaQHJFjkRGpnJyBlBUnyk4ua5hlXOTb9l5HsLIBlVdcWxwF+zJh8Ylwau+mcVF8
b8n86zke88du+xTvXfMDn6p6EACmBncyZGi424hSw72u8jS0cdmqJl3isLR6duG3
4yipWhEpLU5YuR+796jmjK5h+HQwl/Ra2dykbAw7vN0ofdK0+7LrVnGh7dDecOGK
0fElgFPTazeQQV+dEzqz6UwO0Z9koxqBwPqCLi7sXOeUWwqqb3ewYO7TMM4NlK/o
C8oG4yvWj9pLAoIBACEI9PHhbSkj5wqJ8OwyA3jUfdlJK0hAn5PE0GGUsClVIJwU
ggd7aoMyZMt+3iqjvyat8QIjSo6EkyEacmqnGZCoug9FKyM975JIj2PPUOVb29Sq
ebTVS3BeMXuBxhaBeDBS+GypamgNPH8lKKHFFWMdBaEqcXTUU1i0bgxViJE8C/xk
o8VLPB7nr1YtpZvhaSVACOprZd3Xi41zgkoCEXNdomsUFdEgQL+TnCY7Jcnu/NfQ
8xyWe58Si98jMwl1DVqqu2ijk/Z27Ay4TcweeJrfLGWpRTukFROXiNJ2SMzd7Bh5
Gns9Bz3vgdiJDAzx7JOeCw6LfycbPIpWKDAE4qECggEAZ5kPG7H4Dcio6iPwsj1M
eSXBwc/S5C58FTvYXtERT7o+0T2r8FMIKl1+52vr4Qo6LFLpaaxIh5GNCFE5JJ2o
wbi1UwUFRGVjrBJl7QA4ZHJnoE2wr8673rCCui21V15g637PT4kIqG6OrFaBk6oa
MadDZVfJoX+5QQru8QOGJRQPX3h0/L8zlsKO33gxBId2bQs+E8Mr761G3Wko7nge
HbHZVWet6IC0CHbZ15y7F5APQVt3oR/83tfnughlSQgLBPK/l/F1CsaMlAYG0nB6
Q0/USAsS0FfJBgJX8nY12uMG9OPhbRf2i0O2Nk61JobA2PS7XTUF3pT9/naOiCDX
zwKCAQBK9dPzoc9CfdAJVWjCqR2xS8Jr3/64k59Pya6e7Y4ca+e1g4N7hhQRLPW7
owTKloXrR0mAkwOIiJlk+gXsl2banuSajiPxumSfPYWE1Q/QNFD/WoPvo6rPYJ8N
yA/ORsMjWq51SfpzOU69+FdY7p3GvIVWhRtinqseaAIMOkNZBLVDXF4DvtFgiLZM
bKAjGuXsKOT3MPFU9tHxi4q/7flUb30mSUVXyPjh+C+UH7e0BS0pi/rDeRdEju4z
bJVERP8/VAJ61TDQJq+Il95fzKe4yTA3dDHnO+EG5W2eCsawTK4Ze5XAWqomgdew
62D3AkJQiflLfJL8zTFph1FZXLOm
-----END PRIVATE KEY-----`

// ca.rqlite.io
const caPrivateKey = `-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----`

// ca.rqlite.io
const caCert = `-----BEGIN CERTIFICATE-----
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
-----END CERTIFICATE-----`

// server.rqlite.io
const serverPrivateKey = `-----BEGIN PRIVATE KEY-----
MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBALFE+GL/vrTAS3n6
pqXrDQ6ktdu+9QTs3xebqR1TToMuJEh68FgPDgrP1OlyZ6IarrhV/T4EAkYM+Rel
Y+G1HOVg/Ln8mMasWQvFQbNIL5UdIJN9o1F3jnrvWOuzdW+PdIgxIMeeT3jBajy9
DwXvB4aJLg29qKVld8+kaS3LydEjAgMBAAECgYAR7j0FnoLGu7CmvPDAVZ8zfPuc
J5uXDPKf67HWcoe9gxxObIkFDzjHZTBPELBk2DgWEzSTYkpslYFYn/UTboNmkQEs
9KOykpdMU2r6T1oAYHQvCmRUIDvqgxwR1MlqOOu/XBQqZ89Vfk7bXfhCaeuRHU6a
slKSXbtMf49M7DKR6QJBAOf/02NuFT9fWRm80yiFp9LO/s1iU/BLDG7lNQoqYeP/
8wuGKngZewZ6KUGNR+1PTCk8c6DyI2VL+y+5zvToFJ0CQQDDm7NIKMEEK6xPGA9X
TPHeSC1wmMnCHaxAUsZEwrE0b2+AOHdhsWc12JH2tpiAY1gJ1gdJ4sa6tcPk4PAG
STC/AkEAtjv+uRCkggYMgCoRl7f9DptoDL6a/pqE5qsGkbie8jB/omK+A17Ig1r8
AzDN+fua8J06mob5BL29Tkze4wNWBQJBAILPdfZ0opeaaTG/okq8fycqV5Dr7Ejv
NQkTEdpb5MtvFj7GBDgFvkLJINu/Qn7hcLerNNaZXFLySR2fu4RIn9sCQQDa6L0v
C2vfskOO0k+JPn+oLI6F2Sj9xuwjMp+6uuvPTtgQD/az9jeY1ObJZFUasqr1MLrf
hUtjnza1Zwoj2iET
-----END PRIVATE KEY-----`

// server.rqlite.io
const serverCASignedCert = `Certificate:
Data:
    Version: 1 (0x0)
    Serial Number: 4660 (0x1234)
    Signature Algorithm: md5WithRSAEncryption
    Issuer: C=US, ST=Pennsylvania, L=Mars, O=rqlite , OU=IT Department, CN=ca.rqlite.io
    Validity
        Not Before: Feb 28 02:45:54 2023 GMT
        Not After : Feb 23 02:45:54 2043 GMT
    Subject: C=US, ST=Pennsylvania, L=Mars, O=rqlite , OU=IT Department, CN=server.rqlite.io
    Subject Public Key Info:
        Public Key Algorithm: rsaEncryption
            RSA Public-Key: (1024 bit)
            Modulus:
                00:b1:44:f8:62:ff:be:b4:c0:4b:79:fa:a6:a5:eb:
                0d:0e:a4:b5:db:be:f5:04:ec:df:17:9b:a9:1d:53:
                4e:83:2e:24:48:7a:f0:58:0f:0e:0a:cf:d4:e9:72:
                67:a2:1a:ae:b8:55:fd:3e:04:02:46:0c:f9:17:a5:
                63:e1:b5:1c:e5:60:fc:b9:fc:98:c6:ac:59:0b:c5:
                41:b3:48:2f:95:1d:20:93:7d:a3:51:77:8e:7a:ef:
                58:eb:b3:75:6f:8f:74:88:31:20:c7:9e:4f:78:c1:
                6a:3c:bd:0f:05:ef:07:86:89:2e:0d:bd:a8:a5:65:
                77:cf:a4:69:2d:cb:c9:d1:23
            Exponent: 65537 (0x10001)
Signature Algorithm: md5WithRSAEncryption
     d3:43:98:37:be:44:43:57:24:d2:b2:a2:91:19:a1:03:fc:db:
     39:64:49:e9:80:c2:47:0a:32:fe:c2:45:b8:8e:4d:bc:05:00:
     42:81:0e:82:58:4d:29:bf:5b:58:00:c4:a7:ac:19:22:ec:6c:
     35:b3:85:ed:5f:ba:05:32:9b:37:d5:3d:78:e7:a6:f7:93:28:
     c0:81:a6:2f:73:d6:3b:cb:75:2d:92:5c:88:00:40:65:40:0a:
     1c:e4:3f:4f:22:5e:9b:de:9d:46:d3:7c:3b:7b:a7:dd:5e:4a:
     63:e4:ab:b8:e6:13:25:78:9c:d6:bd:c0:4b:20:80:8c:06:89:
     16:19:77:d3:13:e9:62:22:bc:93:25:f4:0a:1a:87:27:c1:6a:
     60:e2:a4:78:70:45:2d:fa:be:6d:8d:b7:fd:77:79:65:f4:16:
     46:ca:b7:6c:96:fb:53:92:14:27:87:63:72:01:90:b6:b2:5a:
     65:f4:ea:40:63:a7:1b:19:6a:14:b3:3b:e8:fc:3f:2c:db:80:
     d3:7c:4e:74:5e:60:1b:01:22:1e:53:a6:b8:be:93:91:f0:b7:
     0b:24:a3:0d:f5:ea:51:87:fb:19:ab:16:67:a1:4b:c1:2f:ca:
     0c:61:f3:4e:99:fd:1b:13:d0:ea:a3:c2:6d:fb:a0:cb:ef:25:
     3b:1d:78:0f:3c:ec:6e:c8:da:a3:32:9e:4d:1e:2b:8f:20:4d:
     92:64:87:67:73:57:0c:b7:f9:8c:ff:4d:0a:a2:2a:78:e4:dc:
     ed:af:ec:cc:b2:06:93:57:6c:a7:b8:21:f3:10:6d:0d:f5:18:
     2c:a9:3d:e5:3f:e6:2e:9f:ff:f6:23:50:f2:88:c4:7b:0b:e6:
     13:cb:77:ee:29:b0:f7:98:a5:c5:f9:09:34:d5:55:b8:ae:f2:
     72:a1:89:08:4d:c6:0b:da:0a:38:ef:74:4f:a9:4b:50:8b:3f:
     11:7a:6e:8e:51:b8:02:0b:f5:6b:a3:e5:76:fb:a1:24:b3:8f:
     cd:eb:20:80:f9:c2:60:e3:7f:a4:d5:dc:99:e1:65:d4:5b:a0:
     43:28:61:ed:f8:e5:9f:e8:20:c9:f6:b5:3a:ca:00:d6:ac:b9:
     21:ab:86:df:cc:5c:cb:68:75:16:4a:8e:06:a5:cd:c0:f2:d8:
     e5:77:d1:d9:fb:c7:1e:ce:f9:52:af:59:89:1d:f1:05:c3:99:
     30:8a:14:30:41:f7:96:45:55:26:65:d1:28:60:47:f4:63:6a:
     53:2c:87:9e:dc:af:c6:93:12:fa:61:46:c1:7c:ab:2f:1f:87:
     11:c4:af:a4:8e:c0:11:a9:34:35:22:3c:00:c4:af:16:1e:5c:
     56:18:37:f4:59:25:db:76
-----BEGIN CERTIFICATE-----
MIID3TCCAcUCAhI0MA0GCSqGSIb3DQEBBAUAMHQxCzAJBgNVBAYTAlVTMRUwEwYD
VQQIDAxQZW5uc3lsdmFuaWExDTALBgNVBAcMBE1hcnMxEDAOBgNVBAoMB3JxbGl0
ZSAxFjAUBgNVBAsMDUlUIERlcGFydG1lbnQxFTATBgNVBAMMDGNhLnJxbGl0ZS5p
bzAeFw0yMzAyMjgwMjQ1NTRaFw00MzAyMjMwMjQ1NTRaMHgxCzAJBgNVBAYTAlVT
MRUwEwYDVQQIDAxQZW5uc3lsdmFuaWExDTALBgNVBAcMBE1hcnMxEDAOBgNVBAoM
B3JxbGl0ZSAxFjAUBgNVBAsMDUlUIERlcGFydG1lbnQxGTAXBgNVBAMMEHNlcnZl
ci5ycWxpdGUuaW8wgZ8wDQYJKoZIhvcNAQEBBQADgY0AMIGJAoGBALFE+GL/vrTA
S3n6pqXrDQ6ktdu+9QTs3xebqR1TToMuJEh68FgPDgrP1OlyZ6IarrhV/T4EAkYM
+RelY+G1HOVg/Ln8mMasWQvFQbNIL5UdIJN9o1F3jnrvWOuzdW+PdIgxIMeeT3jB
ajy9DwXvB4aJLg29qKVld8+kaS3LydEjAgMBAAEwDQYJKoZIhvcNAQEEBQADggIB
ANNDmDe+RENXJNKyopEZoQP82zlkSemAwkcKMv7CRbiOTbwFAEKBDoJYTSm/W1gA
xKesGSLsbDWzhe1fugUymzfVPXjnpveTKMCBpi9z1jvLdS2SXIgAQGVAChzkP08i
XpvenUbTfDt7p91eSmPkq7jmEyV4nNa9wEsggIwGiRYZd9MT6WIivJMl9AoahyfB
amDipHhwRS36vm2Nt/13eWX0FkbKt2yW+1OSFCeHY3IBkLayWmX06kBjpxsZahSz
O+j8PyzbgNN8TnReYBsBIh5Tpri+k5Hwtwskow316lGH+xmrFmehS8Evygxh806Z
/RsT0Oqjwm37oMvvJTsdeA887G7I2qMynk0eK48gTZJkh2dzVwy3+Yz/TQqiKnjk
3O2v7MyyBpNXbKe4IfMQbQ31GCypPeU/5i6f//YjUPKIxHsL5hPLd+4psPeYpcX5
CTTVVbiu8nKhiQhNxgvaCjjvdE+pS1CLPxF6bo5RuAIL9Wuj5Xb7oSSzj83rIID5
wmDjf6TV3JnhZdRboEMoYe345Z/oIMn2tTrKANasuSGrht/MXMtodRZKjgalzcDy
2OV30dn7xx7O+VKvWYkd8QXDmTCKFDBB95ZFVSZl0ShgR/RjalMsh57cr8aTEvph
RsF8qy8fhxHEr6SOwBGpNDUiPADErxYeXFYYN/RZJdt2
-----END CERTIFICATE-----`

// client.rqlite.io
const clientPrivateKey = `-----BEGIN PRIVATE KEY-----
MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBAL7clMXM7/4QGTvm
ShjaCcoUO2zWabFb6fbg4gIioJPSXlRc0AECXgqXDAbMfzb8puwqx4U7vYzJTMd7
pIWRPq2xsink3qsYpBgHjKo8S2YpRZ9kDmpoiAt1RUqMTtWP7K/IhtNZhTObJHUR
7TzXlmt7Tc3LAvFn7WkDizfJ4tLfAgMBAAECgYArRUMNXRsD1I6D///IhpY1lESs
tiecKCRw7icPKN6S5Nyx76DQucKsT/ZQDEjDJKCLZl95m0OsCW84wpVYGsfEoMxQ
xesoUsdzXW1bEBsdQOMY3sAE+uxysOHJ0Lj6PEnVEzde1Arg3vCxIPxKFyOFUEwp
mVm4rFxhqsHX4UdUcQJBAO02QMKvyKLaVdXbnSnBBDUHl5fplMugFsgqVNotqXNC
I3PrdFsLOcdgbgXaa/gTJXEVHrucs6UR8Wsm/x44u4sCQQDN+oW/htI7EqwoyeNR
FOL4eYvZXGwdsPcH7rOgyGNULes/nQY8gjr+S92qs2nQfsBorx+Cky0U0YIEU8Xv
dcB9AkEA5KfIUyJo73oxBV2VqHrzGD8CRKAXGxVtAHO1qT4cugqF7CaJ2Xz/rA1q
4N+D9fRWwiOOpWBO1o5uPVCw2KvtMwJBAJ0Hz+2K8D1e5+cUuvs2jC7YIxjrz/T2
0+21OjZqbudfNojBwl5g/m6eEfkwXIw6BaAJWmFmqKjFbHS2FSGQyNUCQQCDFgih
ciwlv3LZDvQYAcd8i19GHQnzKqb4XMbxRGiWpfaYn4SnDguKJqvvR8b8wY7Of3ne
9Stn2x5S22Cr4H/g
-----END PRIVATE KEY-----`

// client.rqlite.io
const clientCASignedCert = `Certificate:
Data:
    Version: 1 (0x0)
    Serial Number: 4661 (0x1235)
    Signature Algorithm: md5WithRSAEncryption
    Issuer: C=US, ST=Pennsylvania, L=Mars, O=rqlite , OU=IT Department, CN=ca.rqlite.io
    Validity
        Not Before: Feb 28 02:45:54 2023 GMT
        Not After : Feb 23 02:45:54 2043 GMT
    Subject: C=US, ST=Pennsylvania, L=Mars, O=rqlite , OU=IT Department, CN=client.rqlite.io
    Subject Public Key Info:
        Public Key Algorithm: rsaEncryption
            RSA Public-Key: (1024 bit)
            Modulus:
                00:be:dc:94:c5:cc:ef:fe:10:19:3b:e6:4a:18:da:
                09:ca:14:3b:6c:d6:69:b1:5b:e9:f6:e0:e2:02:22:
                a0:93:d2:5e:54:5c:d0:01:02:5e:0a:97:0c:06:cc:
                7f:36:fc:a6:ec:2a:c7:85:3b:bd:8c:c9:4c:c7:7b:
                a4:85:91:3e:ad:b1:b2:29:e4:de:ab:18:a4:18:07:
                8c:aa:3c:4b:66:29:45:9f:64:0e:6a:68:88:0b:75:
                45:4a:8c:4e:d5:8f:ec:af:c8:86:d3:59:85:33:9b:
                24:75:11:ed:3c:d7:96:6b:7b:4d:cd:cb:02:f1:67:
                ed:69:03:8b:37:c9:e2:d2:df
            Exponent: 65537 (0x10001)
Signature Algorithm: md5WithRSAEncryption
     af:9c:f1:ed:52:f1:07:33:fd:ee:45:65:46:00:13:af:40:12:
     d9:68:9b:27:ff:2b:10:4f:8f:43:fa:35:8f:96:43:87:87:b4:
     ba:c6:e8:ee:9d:70:3a:36:83:14:e7:e4:43:87:cc:f8:9c:4e:
     0d:20:e1:34:ca:56:2f:fb:e5:98:05:0d:24:8b:dc:d7:5f:3b:
     00:c3:81:5f:68:87:ec:29:81:96:62:fe:18:f5:f5:55:ba:bb:
     72:8f:09:c4:8b:9b:34:53:b4:e0:3e:ec:9e:74:3c:e9:08:7c:
     cb:2d:c8:d8:99:1d:4a:88:79:56:9d:20:60:8e:ca:f5:50:39:
     62:d0:56:40:55:d3:93:77:c0:8b:14:3c:fd:0b:66:16:86:63:
     c0:85:c5:c8:3a:3e:7c:39:f4:98:c5:3c:aa:27:6c:e0:92:2f:
     7c:d2:39:26:74:7c:87:53:34:b1:cb:b7:2d:90:be:fd:f2:19:
     31:95:ad:ee:d7:20:0c:76:a4:8e:f4:15:e2:32:0b:06:9b:78:
     84:60:a8:9b:6e:e8:90:5d:c5:9f:1e:67:e8:78:80:6c:83:be:
     9b:c5:be:a3:2a:ca:2a:ba:2d:35:1b:ed:38:02:c7:f7:2a:03:
     bd:97:c9:e2:25:18:3b:5b:e8:fb:9a:f5:58:4c:d9:d5:1a:a7:
     18:8e:3e:e0:66:9a:72:f3:f7:b1:01:ec:82:26:1b:56:c8:81:
     2e:6e:06:38:f8:94:45:9e:5a:4c:fa:d8:dd:77:c0:74:26:c5:
     9a:e4:c2:0a:58:6b:8b:88:61:0e:92:ad:52:ed:27:a6:7a:16:
     1f:6a:07:c0:d9:31:83:7d:a5:11:91:89:50:06:b9:4a:0f:3b:
     8d:a9:33:e2:c7:a7:a7:b3:00:b8:0e:7f:2d:09:37:e0:21:75:
     28:75:e5:e5:eb:0a:73:d1:47:f7:a4:d5:31:5d:51:75:bb:da:
     5f:8c:f0:e8:b4:4a:ed:c1:9a:14:a0:2c:0e:98:9a:2d:ae:ea:
     d8:7a:a8:41:28:bd:65:67:71:ca:2d:37:af:5a:f0:5a:62:31:
     01:b2:4f:09:eb:48:0b:9f:a1:a3:90:f0:a6:dc:26:54:51:68:
     8b:49:a6:0a:0f:f2:eb:e9:1a:36:20:f6:f1:3c:66:89:2e:bb:
     a0:a3:34:01:aa:f8:c6:6f:80:09:ac:15:06:0c:d5:70:f5:86:
     9d:c4:ca:7c:e3:3e:11:5d:9b:4a:22:cb:7e:25:b9:2d:3a:f5:
     a7:d1:56:06:1b:27:a7:1a:63:1e:e0:7d:84:83:af:88:b2:be:
     c2:99:4e:3c:08:58:09:d9:35:fe:7c:46:46:6d:e2:68:db:58:
     bf:20:72:4f:cc:8d:5f:17
-----BEGIN CERTIFICATE-----
MIID3TCCAcUCAhI1MA0GCSqGSIb3DQEBBAUAMHQxCzAJBgNVBAYTAlVTMRUwEwYD
VQQIDAxQZW5uc3lsdmFuaWExDTALBgNVBAcMBE1hcnMxEDAOBgNVBAoMB3JxbGl0
ZSAxFjAUBgNVBAsMDUlUIERlcGFydG1lbnQxFTATBgNVBAMMDGNhLnJxbGl0ZS5p
bzAeFw0yMzAyMjgwMjQ1NTRaFw00MzAyMjMwMjQ1NTRaMHgxCzAJBgNVBAYTAlVT
MRUwEwYDVQQIDAxQZW5uc3lsdmFuaWExDTALBgNVBAcMBE1hcnMxEDAOBgNVBAoM
B3JxbGl0ZSAxFjAUBgNVBAsMDUlUIERlcGFydG1lbnQxGTAXBgNVBAMMEGNsaWVu
dC5ycWxpdGUuaW8wgZ8wDQYJKoZIhvcNAQEBBQADgY0AMIGJAoGBAL7clMXM7/4Q
GTvmShjaCcoUO2zWabFb6fbg4gIioJPSXlRc0AECXgqXDAbMfzb8puwqx4U7vYzJ
TMd7pIWRPq2xsink3qsYpBgHjKo8S2YpRZ9kDmpoiAt1RUqMTtWP7K/IhtNZhTOb
JHUR7TzXlmt7Tc3LAvFn7WkDizfJ4tLfAgMBAAEwDQYJKoZIhvcNAQEEBQADggIB
AK+c8e1S8Qcz/e5FZUYAE69AEtlomyf/KxBPj0P6NY+WQ4eHtLrG6O6dcDo2gxTn
5EOHzPicTg0g4TTKVi/75ZgFDSSL3NdfOwDDgV9oh+wpgZZi/hj19VW6u3KPCcSL
mzRTtOA+7J50POkIfMstyNiZHUqIeVadIGCOyvVQOWLQVkBV05N3wIsUPP0LZhaG
Y8CFxcg6Pnw59JjFPKonbOCSL3zSOSZ0fIdTNLHLty2Qvv3yGTGVre7XIAx2pI70
FeIyCwabeIRgqJtu6JBdxZ8eZ+h4gGyDvpvFvqMqyiq6LTUb7TgCx/cqA72XyeIl
GDtb6Pua9VhM2dUapxiOPuBmmnLz97EB7IImG1bIgS5uBjj4lEWeWkz62N13wHQm
xZrkwgpYa4uIYQ6SrVLtJ6Z6Fh9qB8DZMYN9pRGRiVAGuUoPO42pM+LHp6ezALgO
fy0JN+AhdSh15eXrCnPRR/ek1TFdUXW72l+M8Oi0Su3BmhSgLA6Ymi2u6th6qEEo
vWVnccotN69a8FpiMQGyTwnrSAufoaOQ8KbcJlRRaItJpgoP8uvpGjYg9vE8Zoku
u6CjNAGq+MZvgAmsFQYM1XD1hp3EynzjPhFdm0oiy34luS069afRVgYbJ6caYx7g
fYSDr4iyvsKZTjwIWAnZNf58RkZt4mjbWL8gck/MjV8X
-----END CERTIFICATE-----`
