// Package scan holds what the errorrules scanners share: loading the server with types, failing
// the run on any package that does not load, the files a scan skips, and positions printed
// relative to the directory the tool runs from (the repo root).
package scan

import (
	"errors"
	"fmt"
	"go/token"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Load type-checks the packages matching patterns. Any package that fails to load fails the
// whole load: a scan that silently skips a package reports nothing about it.
func Load(patterns []string) ([]*packages.Package, error) {
	cfg := &packages.Config{
		Mode:       packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		BuildFlags: []string{"-tags=disable_grpc_modules"},
	}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		return nil, fmt.Errorf("load: %w", err)
	}
	var errs []error
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			errs = append(errs, fmt.Errorf("package error: %w", e))
		}
	})
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return pkgs, nil
}

// IsTestFile reports whether a file belongs to tests or test helpers, which the scans skip.
func IsTestFile(name string) bool {
	return strings.HasSuffix(name, "_test.go") || strings.Contains(name, "/tests/") || strings.Contains(name, "/testing/")
}

// Pos prints a position as file:line, with the file relative to the working directory.
func Pos(p token.Position) string {
	return fmt.Sprintf("%s:%d", relFile(p.Filename), p.Line)
}

func relFile(name string) string {
	wd, err := os.Getwd()
	if err != nil {
		return name
	}
	rel, err := filepath.Rel(wd, name)
	if err != nil || strings.HasPrefix(rel, "..") {
		return name // outside the working directory: print it whole
	}
	return rel
}

// RequireScanned returns an error when a scan covered too few files to mean anything.
func RequireScanned(files int) error {
	if files < 1000 {
		return fmt.Errorf("suspiciously little scanned: %d files", files)
	}
	return nil
}
