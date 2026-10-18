FROM cimg/go:1.27

# Become root for package installs
USER root
ARG DEBIAN_FRONTEND=noninteractive

# System deps and cross-compilers in one layer
RUN set -eux; \
    apt-get update; \
    apt-get install -y \
      ca-certificates curl \
      python3 python3-pip python3-venv \
      nodejs npm \
      musl-tools musl-dev \
      gcc-mingw-w64-x86-64-posix \
      gcc-arm-linux-gnueabi \
      gcc-aarch64-linux-gnu \
      gcc-riscv64-linux-gnu \
      gcc-powerpc64le-linux-gnu \
      gcc-mips-linux-gnu \
      gcc-mipsel-linux-gnu \
      gcc-mips64-linux-gnuabi64 \
      gcc-mips64el-linux-gnuabi64; \
    rm -rf /var/lib/apt/lists/*

# Python virtual environment (isolates from Debian’s externally-managed Python)
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv "$VIRTUAL_ENV" && "$VIRTUAL_ENV/bin/pip" install --upgrade pip
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Python dependencies (use the venv's pip)
# For determinism, prefer a requirements.txt with pinned hashes.
RUN pip install --no-cache-dir \
      requests \
      boto3 \
      google-cloud-storage

# Drop privileges back to the default CircleCI user
USER circleci

