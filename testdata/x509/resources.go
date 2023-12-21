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
MIIDjjCCAnagAwIBAgIUQdomCw77L5nEqXe9WKVBu+YNTtUwDQYJKoZIhvcNAQEL
BQAwczELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAkNBMRYwFAYDVQQHDA1TYW4gRnJh
bmNpc2NvMRMwEQYDVQQKDApNeSBDb21wYW55MRQwEgYDVQQLDAtNeSBEaXZpc2lv
bjEUMBIGA1UEAwwLZXhhbXBsZS5jb20wHhcNMjMxMjIxMTMyNzUzWhcNMjQxMjIw
MTMyNzUzWjBzMQswCQYDVQQGEwJVUzELMAkGA1UECAwCQ0ExFjAUBgNVBAcMDVNh
biBGcmFuY2lzY28xEzARBgNVBAoMCk15IENvbXBhbnkxFDASBgNVBAsMC015IERp
dmlzaW9uMRQwEgYDVQQDDAtleGFtcGxlLmNvbTCCASIwDQYJKoZIhvcNAQEBBQAD
ggEPADCCAQoCggEBALSBJKK21N5pxrkcNOUun0VWZmSQr/CgGv0bYMoxHQhsa+Sr
zvDom+4+tXrlDwEqtpaUOC4HPTZHZoPJPcII9lpLwxRcFX7foyZ0N5IDgF5Dg1o/
1Wrujh/fQJ0TMjWua5VxAhRBWlY2h3uEe482wDC9d0aVBEFG6agaKcV6NaKs+VLX
lF8raVYsckjm2ad99wDg8WBAhyLKoEo+2Rfnpj0V4O3G3gK3CTR/QXXT3lTWPVeR
gcQNzhA7oPrrgp0jKpWdjfKp7SokfAYYelzs155Zam7YX3danHZb6JHdYPYco82U
R8vkjIj0LvuteKNb4VQfIj63HJy6X84blLpmbl8CAwEAAaMaMBgwFgYDVR0RBA8w
DYILZXhhbXBsZS5jb20wDQYJKoZIhvcNAQELBQADggEBAD0lAd964YLyg0HCigC8
GoPlPAtkFwhFwRaklm7LhZQOfgS1QruOtm1ds2PuSSr787L/tInWogp2SO1tCLK+
O+2bu0w59UhKc1bK9OWhBk0yLnMVkOIMF4Rp0kgocKVjX12Q/2Gd8Yr6UL4inOUV
r7/VOlekLQLkslsS73udGURmV/2G/90a/QuG8ZOKexLzUFVe/VPOVZ0/Jmxzckg1
W4tevlDIckAcBD6zovycDgY/qDwNyVvGot6N0BxFCSoq1iqg2vgr7zvuz++N7HPi
1mCoxsDmy5i7cEBewtvl+9+yu6OvQdkr7LZpgEyZnyiG/Q6xHxxe3QlQc3WEc0fQ
NPU=
-----END CERTIFICATE-----`

const keyExampleDotCom = `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAtIEkorbU3mnGuRw05S6fRVZmZJCv8KAa/RtgyjEdCGxr5KvO
8Oib7j61euUPASq2lpQ4Lgc9Nkdmg8k9wgj2WkvDFFwVft+jJnQ3kgOAXkODWj/V
au6OH99AnRMyNa5rlXECFEFaVjaHe4R7jzbAML13RpUEQUbpqBopxXo1oqz5UteU
XytpVixySObZp333AODxYECHIsqgSj7ZF+emPRXg7cbeArcJNH9BddPeVNY9V5GB
xA3OEDug+uuCnSMqlZ2N8qntKiR8Bhh6XOzXnllqbthfd1qcdlvokd1g9hyjzZRH
y+SMiPQu+614o1vhVB8iPrccnLpfzhuUumZuXwIDAQABAoIBAQCCd94NJm3SnU4g
On0ZMou4yGyWP+aL3I3laMabHsjHb+bEkFKx2s/qcrUmMS6ZP1Beop/A1UavVwiI
NQtIlsXKGnzU2IJJBEie4N3R0moUuGPW9dDCy886xlmjr15ZYsssZ1SDY4FyU1O2
Odo9d+uOz0PJZqh+rkzxTofmzyC5hWp/QDSwflM63Ddc54oxqXre3d7E4ZIybv4z
/NhjVOeJQ5rcsPuoDyzcvu9zjoZlWyUVhJXxOD9Lsl4m1OtedlCy5pC7EGLfKCX8
2cMa6xyRGG5fkD1p3KtRjDRlCnJWZSfUpqfyBDNyd35ggsU2t1YUjtGOIm02s8Bv
Ppj91spRAoGBAN31QS7hb68KeyXzIg8PTmekuCe3J5pLdqqSJUddRRnuXF3Ck1UG
vIxCTthMDnrwm1zNOKSr8o1yWQiB+D8+wK4BQBWp3d+aPMdmXKrLabyOvBYC9l1k
pmvApgU3mq3Gmo5HItqnKveu77/0f9Phc7+Zx61D0M64GoyxCocxOeclAoGBANAw
S8ON0ly9z9/SeP7827GZe6BTHRosDLIeB4nhj28UkZA5Tqmhl9KltYRKN+elM9lk
dEbpJHKbQq8p9KrMgPMZbA10clp0201Phl+p/gXoV3+tW4ty68LnoHoJ0aVmcJg7
1wi6hQA4TktsuqGrQopxBcP2sfkPxw2wvOsC0zozAoGAIsrMAe5ClHyfGy7Gevfa
QXLXjxotsgj8O/aWs14Sc+MiRWw3rg9VROMr+snJR0oqAF0G8QEnDzcKb8P3xuou
R9hYVE0BQ4io2FTaV8d09qKlJwol1jPtxlw3af/yViUYmw7Zjs6/dH3OhRdtZigj
/be8ThOgSpcfSkiCLvjva6UCgYB5mWv3WbPgNYHgYqgHj5umnVZHK6SY79euhbmz
gErt/56SFdbrX5Y1FFL0ZQFITQb+2BzOekiupB8L/r6IsJA07cleBoDwcsbQ3j1H
TRwxSScqTRBmYAZyviXOgkOwjyf+0xzjXUZn/XtFtMlBuq1P/Xn3nvUO23dReMSv
o+PKXwKBgAIAB8Om2Sn2WtVbEoUSKthdp3C3Uv4rtpuHrChA/CJfznTz+abDI0Lq
65L3OLg5mPkyRQQuOPyo68S9ebuAwfX464G/P3IPPc3wcvvVOKrvgCxRbDzoO90i
XRolXKALnQTRz+4u+Essl9QG8ETWtAPz3De6fJybfSbg616LXr0u
-----END RSA PRIVATE KEY-----`
