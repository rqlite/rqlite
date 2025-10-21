# Pick a release number from protocolbuffers/protobuf releases, e.g. 28.3
PROTOC_VER=33.0
ARCH=$(uname -m)
case "$ARCH" in
  x86_64)  PROTOC_ARCH=x86_64 ;;
  aarch64) PROTOC_ARCH=aarch_64 ;;
  armv7l)  PROTOC_ARCH=armv7 ;;
  *) echo "Unsupported arch: $ARCH"; exit 1 ;;
esac

echo curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VER}/protoc-${PROTOC_VER}-linux-${PROTOC_ARCH}.zip

exit

sudo mkdir -p /usr/local
sudo unzip -o protoc-${PROTOC_VER}-linux-${PROTOC_ARCH}.zip -d /usr/local
rm protoc-${PROTOC_VER}-linux-${PROTOC_ARCH}.zip

# Ensure /usr/local/bin comes before /usr/bin
echo 'export PATH=/usr/local/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

protoc --version
