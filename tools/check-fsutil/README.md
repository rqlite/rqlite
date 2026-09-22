Run from the repository root:

```sh
go run ./tools/check-fsutil
```

The checker prints file, line, and column diagnostics and exits with status 1
if it finds a forbidden use or cannot complete the scan. A clean scan exits 0.
Use `-root /path/to/repository` to select a different source root.

All Go files are parsed, including tests, generated files, testdata, and files
excluded by the current platform or build tags. The `.git` and `vendor`
directories are skipped. No external dependencies are required.

Calls and function references to `os.Rename`, `os.Remove`, and `os.RemoveAll`
are rejected, including aliased and dot imports. Comments, strings, and local
variables shadowing an import are ignored.

The only exceptions are the matching package-level functions in
`internal/fsutil/fsutil.go`: each operation is allowed in its own wrapper and
its `WithRetry` helper. For example, `os.Remove` is allowed in `Remove` and
`RemoveWithRetry`, but not in other functions or methods.

Run the checker's tests with:

```sh
go test ./tools/check-fsutil
```
