#!/bin/bash

# Dependencies
add-apt-repository ppa:git-core/ppa -y
echo "Updating..."
apt-get update
apt-get install -y curl git bison make mercurial

# Go
bash < <(curl -s -S -L https://raw.githubusercontent.com/moovweb/gvm/master/binscripts/gvm-installer)
source ~/.gvm/scripts/gvm
gvm install go1.10.0
gvm use go1.10.0

# rqlite
mkdir -p rqlite
cd rqlite
export GOPATH=$PWD
go get github.com/rqlite/rqlite/...
ln -s $GOPATH/bin/rqlited /usr/local/bin/rqlited
