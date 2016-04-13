# Contributing to rqlite
rqlite is software, and it goes without say it can always be improved. It's by no means finished -- issues are tracked, and I plan to develop this project further. Pull requests are very welcome.

## Building rqlite
*Building rqlite requires Go 1.4 or later. [gvm](https://github.com/moovweb/gvm) is a great tool for managing your version of Go.*

Download and run rqlite like so (tested on 64-bit Kubuntu 14.04 and OSX):

```bash
mkdir rqlite # Or any directory of your choice.
cd rqlite/
export GOPATH=$PWD
go get -t github.com/otoolep/rqlite/...
$GOPATH/bin/rqlited ~/node.1
```

This starts a rqlite server listening on localhost, port 4001. This single node automatically becomes the leader.

### Speeding up the build process
It can be rather slow to rebuild rqlite, due to the repeated compilation of SQLite support. You can compile and install this library once, so subsequent builds are much faster. To do so, execute the following commands:
```bash
cd $GOPATH
go install github.com/mattn/go-sqlite3
```

## Cloning a fork
If you wish to work with fork of rqlite, your own fork for example, you must still follow the directory structure above. But instead of cloning the main repo, instead clone your fork. You must fork the project if you want to contribute upstream.

Follow the steps below to work with a fork:

```bash
    export GOPATH=$HOME/rqlite
    mkdir -p $GOPATH/src/github.com/otoolep
    cd $GOPATH/src/github.com/otoolep
    git clone git@github.com:<your Github username>/rqlite
```

Retaining the directory structure `$GOPATH/src/github.com/otoolep` is necessary so that Go imports work correctly.
