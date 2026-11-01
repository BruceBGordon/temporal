package common

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTransientPersistenceErrorGroupIsNotSplit enforces a declared invariant statically:
// IsPersistenceTransientError declares ResourceExhausted and Unavailable as one group. Code
// that gives protective handling (backoff, throttle, delay) to ResourceExhausted must also
// handle Unavailable, otherwise a brief blip bypasses the protection.
// See https://github.com/temporalio/temporal/issues/11547.
//
// The check parses source only; it never runs the code it inspects.
func TestTransientPersistenceErrorGroupIsNotSplit(t *testing.T) {
	protective := regexp.MustCompile(`(?i)delay|backoff|throttle|pause|reschedul|ratelimit|wait|sleep`)
	coversGroup := regexp.MustCompile(`IsPersistenceTransientError|serviceerror\.Unavailable`)

	// Reviewed sites where splitting the group is intended. Each entry must still match a real
	// site; a stale entry fails the test so the list cannot silently rot.
	allowed := map[string]string{
		"common/rpc/interceptor/request_error_handler.go:logError": "chooses what to log, not how to retry",
		"service/matching/pri_matcher.go:forwardTasks":             "RPC to a parent partition, not a persistence read",
		"service/matching/pri_matcher.go:forwardPolls":             "RPC to a parent partition, not a persistence read",
	}

	root := ".."
	scanned := 0
	allowedSeen := map[string]bool{}
	var violations []string

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", "vendor", "testdata", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || strings.HasSuffix(path, "_mock.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			return err // a file we cannot parse must fail loudly, not be skipped
		}
		scanned++
		rel, _ := filepath.Rel(root, path)
		text := func(n ast.Node) string {
			return string(src[fset.Position(n.Pos()).Offset:fset.Position(n.End()).Offset])
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			body := text(fn.Body)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				ifStmt, ok := n.(*ast.IfStmt)
				if !ok || !callsIsResourceExhausted(ifStmt.Cond) {
					return true
				}
				cond := text(ifStmt.Cond)
				if !protective.MatchString(text(ifStmt.Body)) && !strings.Contains(cond, "!") {
					return true
				}
				key := rel + ":" + fn.Name.Name
				if _, ok := allowed[key]; ok {
					allowedSeen[key] = true
					return true
				}
				if coversGroup.MatchString(body) {
					return true
				}
				violations = append(violations, rel+":"+strconv.Itoa(fset.Position(ifStmt.Pos()).Line)+" "+fn.Name.Name+": if "+strings.Join(strings.Fields(cond), " "))
				return true
			})
		}
		return nil
	})
	require.NoError(t, err)

	// Guard against passing for the wrong reason: the walk must actually cover the server.
	require.Greater(t, scanned, 1000, "scanned too few files; the check would pass vacuously")
	for key, why := range allowed {
		require.True(t, allowedSeen[key], "allowlist entry %q (%s) no longer matches any site; remove it", key, why)
	}
	require.Empty(t, violations,
		"ResourceExhausted gets protective handling here but Unavailable (same declared group) does not:\n%s",
		strings.Join(violations, "\n"))
}

func callsIsResourceExhausted(e ast.Expr) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "IsResourceExhausted" {
				found = true
			}
		}
		return true
	})
	return found
}
