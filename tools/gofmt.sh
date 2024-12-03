#!/bin/bash
fmtcount=`git ls-files | grep '.go$' | xargs gofmt -l 2>&1 | wc -l`
if [ $fmtcount -gt 0 ]; then
    echo "run 'go fmt ./...' to format your source code."
    exit 1
fi
