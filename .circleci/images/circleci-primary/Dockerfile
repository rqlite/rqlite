FROM cimg/go:1.24.0

RUN sudo curl https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/bin/mc && sudo chmod +x /usr/bin/mc
RUN sudo apt-get update
RUN sudo apt-get install musl-dev musl-tools
RUN sudo apt-get install gcc-mingw-w64-x86-64-posix
RUN sudo apt-get install gcc-arm-linux-gnueabi
RUN sudo apt-get install gcc-aarch64-linux-gnu
RUN sudo apt-get install gcc-riscv64-linux-gnu
RUN sudo apt-get install gcc-powerpc64le-linux-gnu
RUN sudo apt-get install gcc-mips-linux-gnu
RUN sudo apt-get install gcc-mipsel-linux-gnu
RUN sudo apt-get install gcc-mips64-linux-gnuabi64
RUN sudo apt-get install gcc-mips64el-linux-gnuabi64
RUN sudo apt-get install python3 python3-pip
RUN python3 -m pip install requests boto3
