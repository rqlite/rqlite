// Command check-fsutil rejects direct uses of filesystem operations that must
// go through internal/fsutil.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
)

func main() {
	root := flag.String("root", ".", "repository root containing go.mod")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: check-fsutil [-root directory]")
		os.Exit(1)
	}
	n, err := check(*root, os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	if err != nil || n != 0 {
		os.Exit(1)
	}
}

func check(root string, out io.Writer) (int, error) {
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return 0, fmt.Errorf("repository root must contain go.mod: %w", err)
	}
	fset := token.NewFileSet()
	violations := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(fset, path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		for _, pos := range forbiddenUses(file, filepath.ToSlash(rel)) {
			position := fset.PositionFor(pos.pos, false)
			fmt.Fprintf(out, "%s:%d:%d: use fsutil.%s instead of os.%s\n",
				filepath.ToSlash(rel), position.Line, position.Column, pos.operation, pos.operation)
			violations++
		}
		return nil
	})
	return violations, err
}

type violation struct {
	pos       token.Pos
	operation string
}

func forbiddenUses(file *ast.File, path string) []violation {
	imports := make(map[string]bool)
	for _, spec := range file.Imports {
		name, err := strconv.Unquote(spec.Path.Value)
		if err != nil || name != "os" {
			continue
		}
		alias := "os"
		if spec.Name != nil {
			alias = spec.Name.Name
		}
		imports[alias] = true
	}
	// Parser resolution distinguishes local declarations from imported names.
	// Unresolved also excludes selector fields, which are not dot-import uses.
	unresolved := make(map[*ast.Ident]bool)
	for _, id := range file.Unresolved {
		unresolved[id] = true
	}
	var violations []violation
	for _, decl := range file.Decls {
		function := ""
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil {
			function = fn.Name.Name
		}
		ast.Inspect(decl, func(node ast.Node) bool {
			operation := ""
			switch node := node.(type) {
			case *ast.SelectorExpr:
				if id, ok := node.X.(*ast.Ident); ok && id.Obj == nil && imports[id.Name] {
					operation = node.Sel.Name
				}
			case *ast.Ident:
				if imports["."] && unresolved[node] {
					operation = node.Name
				}
			}
			switch operation {
			case "Rename", "Remove", "RemoveAll":
				// These are the implementation boundary, including their retry closures.
				if path == "internal/fsutil/fsutil.go" && file.Name.Name == "fsutil" &&
					(function == operation || function == operation+"WithRetry") {
					return true
				}
				violations = append(violations, violation{node.Pos(), operation})
			}
			return true
		})
	}
	return violations
}
