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

const caPrivateKey = `-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----`

const caCert = `-----BEGIN CERTIFICATE-----
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
-----END CERTIFICATE-----`

const clientPrivateKey = `-----BEGIN PRIVATE KEY-----
MIICdQIBADANBgkqhkiG9w0BAQEFAASCAl8wggJbAgEAAoGBAMYqgBor4+QKK91L
ytC7B5zlIvFDo2Qj36nvyFVxNfgeDs6heVGNjYo09c9FjsN253Sulg71B3EKknlg
8WJSdQlhkXNB2Cks43YvoMN4VS1GDQyDnv+pQAiajydhUZqiggXIiui7TyH27p3a
JaFar5Y6vUz2c5Z2eDQJFDocBpSZAgMBAAECgYADRlKFnDIQ46Yj6wu39U9D5Xl1
WTPLBNi+WysJVmyY5A5EleCGW3t6TDO/sBuS2VUr1XK/Xoc38//Fp7d1c31hmdsO
TbQfkAjazdfd6yO6hJGh0SxirN4/fWJtpGhQ30gVlEwdvtz1CZmjDR4T878TTMyp
R8qWOsWSKyMr6ifRDQJBAOoQBtiJITeb0R6caKAx9dzUahSLWqq8OsOud0/MaLJ0
rDATxJe2o3eSLK2a4J5QXyDDSxWvOLZGqHMIuqUm5SMCQQDYvTCp+d8G6OT16To4
5owzhwWF9S/tCnTPkbr7EvUL9JAb9wNJfJ/uiW/QUrbZcLJJBoRAxNWCyCI6uMCg
xtETAkAwL6R5J6IDBL0EhEa11BM9py0/lYDQ7XdbmatOblKPip14OFmcsijtENbJ
1ryvWvR6ljn6+NvACsPbCs0B+wPpAkAR6VCkO2obABQr3mJZYXQkrfve3ixfwoV0
we5Z4W8u1b8zEG7NG+d7pw/+f1dtEMsrDcbQM3QRoshxYeNJZiTRAkAmWAwZMoyP
l54AWg3m7SZECDt7xJbJ75cm7iOeYR+DMyDAZlGMAHmpM0hXh2saR2Eic+sutn8C
CnfybwStGdQl
-----END PRIVATE KEY-----`

const clientCASignedCert = `Certificate:
    Data:
        Version: 1 (0x0)
        Serial Number: 4660 (0x1234)
        Signature Algorithm: md5WithRSAEncryption
        Issuer: C=US, ST=Pennsylvania, L=Mars, O=rqlite , OU=IT Department, CN=ca.rqlite.io
        Validity
            Not Before: Feb 27 17:06:39 2023 GMT
            Not After : Feb 22 17:06:39 2043 GMT
        Subject: C=US, ST=Pennsylvania, L=Mars, O=rqlite , OU=IT Department, CN=client.rqlite.io
        Subject Public Key Info:
            Public Key Algorithm: rsaEncryption
                RSA Public-Key: (1024 bit)
                Modulus:
                    00:c6:2a:80:1a:2b:e3:e4:0a:2b:dd:4b:ca:d0:bb:
                    07:9c:e5:22:f1:43:a3:64:23:df:a9:ef:c8:55:71:
                    35:f8:1e:0e:ce:a1:79:51:8d:8d:8a:34:f5:cf:45:
                    8e:c3:76:e7:74:ae:96:0e:f5:07:71:0a:92:79:60:
                    f1:62:52:75:09:61:91:73:41:d8:29:2c:e3:76:2f:
                    a0:c3:78:55:2d:46:0d:0c:83:9e:ff:a9:40:08:9a:
                    8f:27:61:51:9a:a2:82:05:c8:8a:e8:bb:4f:21:f6:
                    ee:9d:da:25:a1:5a:af:96:3a:bd:4c:f6:73:96:76:
                    78:34:09:14:3a:1c:06:94:99
                Exponent: 65537 (0x10001)
    Signature Algorithm: md5WithRSAEncryption
         bd:c4:3f:0d:8f:32:4b:4f:70:25:45:be:47:bc:88:08:a5:24:
         74:cc:ee:fe:26:fc:d2:aa:c8:08:27:c6:2c:dd:d8:31:9c:85:
         68:70:0a:c6:74:87:82:d4:81:f7:0a:f0:78:53:7f:42:15:84:
         42:5b:ea:93:44:35:19:a2:5f:84:4a:30:fe:c6:34:93:d9:b4:
         4d:44:29:e8:61:d5:b2:8e:74:24:34:da:e2:9c:3a:9e:5c:8b:
         99:86:21:4d:4a:ea:10:0b:5e:01:01:3f:49:9d:fb:02:be:03:
         c2:de:b0:83:d2:7e:6d:9e:74:c3:85:58:5e:77:9b:19:f9:70:
         9e:8e:ce:9a:a5:23:2e:28:8d:b6:7b:9a:05:a7:cb:c0:32:39:
         1b:46:fb:a7:0a:fe:3a:4f:7d:69:da:e5:d8:65:3b:49:41:7a:
         73:d3:df:f5:fc:63:cf:21:a5:6d:50:31:4a:17:44:17:dd:62:
         98:7a:d1:f3:3c:f7:dd:54:88:20:45:d9:6f:78:cf:ad:2a:3c:
         e3:12:b7:5a:cb:80:f6:95:d6:95:ba:a1:a6:45:93:f8:d9:f4:
         7a:55:f4:87:61:52:04:b2:7f:03:6f:bd:22:49:55:b5:83:f4:
         45:3e:82:13:8f:ac:78:df:0c:33:43:ce:ab:b9:2d:ff:8b:37:
         8d:a1:30:84:2d:e0:3f:05:74:b4:e1:a7:83:1f:e5:d7:23:f5:
         97:31:f3:39:20:3a:7b:bd:3b:f1:04:18:c2:19:98:1a:f2:e9:
         03:d3:4e:6e:58:cc:0c:60:8a:64:b9:e7:56:93:b9:fb:89:d4:
         67:dc:32:cd:aa:5c:2d:6d:b6:39:7a:64:b6:e2:ce:eb:44:d3:
         9b:b4:25:f1:c4:5d:77:54:b2:a4:6a:df:01:82:82:bb:94:15:
         9f:89:e8:64:02:a2:5b:e1:32:f6:91:9b:79:01:f5:ef:fd:81:
         15:65:06:63:9b:42:09:c9:75:53:3e:0c:1c:f4:c3:18:20:c1:
         6f:65:9c:3b:b8:92:c6:1a:a5:36:9e:91:86:fa:5f:b7:f1:5a:
         9d:e4:76:22:72:35:0c:a5:bf:c9:4d:73:22:ff:7f:f2:7b:25:
         6c:7b:c4:e0:26:4b:1c:e2:04:94:05:b7:ab:8f:09:4b:ad:7d:
         4b:b4:45:10:94:6e:56:e4:8c:20:e4:e3:a1:dc:f3:99:17:e8:
         25:08:09:a8:a3:b0:84:19:dc:94:1e:66:ac:c2:ba:a2:67:0f:
         ce:4f:57:28:a3:79:bc:98:d8:20:47:18:68:45:c2:11:9e:dd:
         6c:5c:9d:88:6e:de:5f:88:74:a5:76:12:f9:be:fb:3c:76:a1:
         7f:a5:4c:3c:02:4b:de:d7
-----BEGIN CERTIFICATE-----
MIID3TCCAcUCAhI0MA0GCSqGSIb3DQEBBAUAMHQxCzAJBgNVBAYTAlVTMRUwEwYD
VQQIDAxQZW5uc3lsdmFuaWExDTALBgNVBAcMBE1hcnMxEDAOBgNVBAoMB3JxbGl0
ZSAxFjAUBgNVBAsMDUlUIERlcGFydG1lbnQxFTATBgNVBAMMDGNhLnJxbGl0ZS5p
bzAeFw0yMzAyMjcxNzA2MzlaFw00MzAyMjIxNzA2MzlaMHgxCzAJBgNVBAYTAlVT
MRUwEwYDVQQIDAxQZW5uc3lsdmFuaWExDTALBgNVBAcMBE1hcnMxEDAOBgNVBAoM
B3JxbGl0ZSAxFjAUBgNVBAsMDUlUIERlcGFydG1lbnQxGTAXBgNVBAMMEGNsaWVu
dC5ycWxpdGUuaW8wgZ8wDQYJKoZIhvcNAQEBBQADgY0AMIGJAoGBAMYqgBor4+QK
K91LytC7B5zlIvFDo2Qj36nvyFVxNfgeDs6heVGNjYo09c9FjsN253Sulg71B3EK
knlg8WJSdQlhkXNB2Cks43YvoMN4VS1GDQyDnv+pQAiajydhUZqiggXIiui7TyH2
7p3aJaFar5Y6vUz2c5Z2eDQJFDocBpSZAgMBAAEwDQYJKoZIhvcNAQEEBQADggIB
AL3EPw2PMktPcCVFvke8iAilJHTM7v4m/NKqyAgnxizd2DGchWhwCsZ0h4LUgfcK
8HhTf0IVhEJb6pNENRmiX4RKMP7GNJPZtE1EKehh1bKOdCQ02uKcOp5ci5mGIU1K
6hALXgEBP0md+wK+A8LesIPSfm2edMOFWF53mxn5cJ6OzpqlIy4ojbZ7mgWny8Ay
ORtG+6cK/jpPfWna5dhlO0lBenPT3/X8Y88hpW1QMUoXRBfdYph60fM8991UiCBF
2W94z60qPOMSt1rLgPaV1pW6oaZFk/jZ9HpV9IdhUgSyfwNvvSJJVbWD9EU+ghOP
rHjfDDNDzqu5Lf+LN42hMIQt4D8FdLThp4Mf5dcj9Zcx8zkgOnu9O/EEGMIZmBry
6QPTTm5YzAxgimS551aTufuJ1GfcMs2qXC1ttjl6ZLbizutE05u0JfHEXXdUsqRq
3wGCgruUFZ+J6GQColvhMvaRm3kB9e/9gRVlBmObQgnJdVM+DBz0wxggwW9lnDu4
ksYapTaekYb6X7fxWp3kdiJyNQylv8lNcyL/f/J7JWx7xOAmSxziBJQFt6uPCUut
fUu0RRCUblbkjCDk46Hc85kX6CUICaijsIQZ3JQeZqzCuqJnD85PVyijebyY2CBH
GGhFwhGe3WxcnYhu3l+IdKV2Evm++zx2oX+lTDwCS97X
-----END CERTIFICATE-----`
