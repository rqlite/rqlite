# Contributing to rqlite
rqlite is software, and it goes without saying it can always be improved. It's by no means finished -- issues are tracked, and I plan to develop this project further. Pull requests are very welcome.

rqlite can be compiled and executed on Linux, OSX, and Microsoft Windows.

## Google Group
You may also wish to check out the [rqlite Google Group](https://groups.google.com/forum/#!forum/rqlite).

## Clean commit histories
If you open a pull request, please ensure the commit history is clean. Squash the commits into logical blocks, perhaps a single commit if that makes sense. What you want to avoid is commits such as "WIP" and "fix test" in the history. This is so we keep history on master clean and straightforward.

## Third-party libraries
Please avoid using libaries other than those available in the standard library, unless absolutely necessary. This requirement is relaxed somewhat for software other than rqlite node software itself. To understand why this approach is taken, check out this [post](https://blog.gopheracademy.com/advent-2014/case-against-3pl/).

## Building rqlite
*Building rqlite requires Go 1.10 or later. [gvm](https://github.com/moovweb/gvm) is a great tool for installing and managing your versions of Go.*

Download, build, and run rqlite like so (tested on 64-bit Kubuntu 16.0 and OSX):

```bash
mkdir rqlite # Or any directory of your choice.
cd rqlite/
export GOPATH=$PWD
go get -u -t github.com/rqlite/rqlite/...
$GOPATH/bin/rqlited ~/node.1
```
This starts a rqlite server listening on localhost, port 4001. This single node automatically becomes the leader.

To rebuild, perhaps after making some changes to the source, do something like the following:
```bash
cd $GOPATH/src/github.com/rqlite/rqlite
go install ./...
$GOPATH/bin/rqlited ~/node.1
```

### Raspberry Pi
The process outlined above will work for Linux, OSX, and Windows. For Raspberry Pi, check out [this issue](https://github.com/rqlite/rqlite/issues/340).

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
mkdir -p $GOPATH/src/github.com/rqlite
cd $GOPATH/src/github.com/rqlite
git clone git@github.com:<your Github username>/rqlite
```

Retaining the directory structure `$GOPATH/src/github.com/rqlite` is necessary so that Go imports work correctly.

## Testing
Be sure to run the unit test suite before opening a pull request. An example test run is shown below.
```bash
$ cd $GOPATH/src/github.com/rqlite/rqlite
$ go test ./...
?       github.com/rqlite/rqlite       [no test files]
ok      github.com/rqlite/rqlite/auth  0.001s
?       github.com/rqlite/rqlite/cmd/rqlite    [no test files]
?       github.com/rqlite/rqlite/cmd/rqlited   [no test files]
ok      github.com/rqlite/rqlite/db    0.769s
ok      github.com/rqlite/rqlite/http  0.006s
ok      github.com/rqlite/rqlite/store 6.117s
ok      github.com/rqlite/rqlite/system_test   7.853s
```

