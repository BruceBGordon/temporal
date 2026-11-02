// lockheld lists functions that acquire a lock or semaphore and, while holding it, call into the
// persistence layer. Such a hold is bounded only by the persistence call's context deadline, and
// only if the database driver honors that deadline (lib/pq does not when the server goes silent;
// see issue #12246). Intra-procedural: it sees calls made directly in the function that takes
// the lock. Run from the repo root: lockheld ./...
package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"slices"
	"strings"

	"go.temporal.io/server/cmd/tools/errorrules/internal/scan"
	"golang.org/x/tools/go/packages"
)

const persistencePrefix = "go.temporal.io/server/common/persistence"

// acquire names: methods that take a lock or a semaphore token.
// releaseFor pairs each acquire with its release; a release ends the hold only on the same object.
var releaseFor = map[string]string{
	"Lock": "Unlock", "RLock": "RUnlock", "Acquire": "Release", "TryLock": "Unlock",
	"wLock": "wUnlock", "rLock": "rUnlock", "ioSemaphoreAcquire": "ioSemaphoreRelease", "LockWithPriority": "Unlock",
}

// ioReceiver: methods on these persistence types do I/O (stores, managers, queues, DB handles);
// serializers and blob helpers in the same packages do not.
func ioReceiver(t types.Type) bool {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	n, ok := t.(*types.Named)
	if !ok {
		return false
	}
	name := n.Obj().Name()
	for _, suf := range []string{"Manager", "Store", "DB", "Tx", "Queue"} {
		if strings.HasSuffix(name, suf) {
			return true
		}
	}
	return false
}

var acquireNames = map[string]bool{
	"Lock": true, "RLock": true, "Acquire": true, "TryLock": true,
	"wLock": true, "rLock": true, "ioSemaphoreAcquire": true, "LockWithPriority": true,
}

type finding struct {
	pos, fn, lock string
	calls         []string
}

// knownHolders are the shard's io-semaphore holders behind #12246. The scan must find them, or it
// is checking nothing.
var knownHolders = []string{"(*ContextImpl).AssertOwnership", "(*ContextImpl).UpdateWorkflowExecution", "(*ContextImpl).updateShardInfo"}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2) // the scan itself is broken
	}
}

// run scans the packages matching patterns and prints what it finds.
func run(patterns []string) error {
	pkgs, err := scan.Load(patterns)
	if err != nil {
		return err
	}
	var out []finding
	files := 0
	for _, p := range pkgs {
		if !strings.HasPrefix(p.PkgPath, "go.temporal.io/server") || strings.HasPrefix(p.PkgPath, persistencePrefix) {
			continue // the persistence layer itself is the callee, not a holder
		}
		n, found := scanPackage(p)
		files += n
		out = append(out, found...)
	}
	if err := scan.RequireScanned(files); err != nil {
		return err
	}
	slices.SortFunc(out, func(a, b finding) int { return strings.Compare(a.pos, b.pos) })
	if err := requireKnownHolders(out); err != nil {
		return err
	}
	fmt.Printf("scanned %d files; %d functions hold a lock across a persistence call\n\n", files, len(out))
	for _, f := range out {
		fmt.Printf("%s %s\n    holds %s across: %s\n", f.pos, f.fn, f.lock, strings.Join(f.calls, ", "))
	}
	return nil
}

// scanPackage scans the package's production files and returns how many it scanned and the
// holds it found.
func scanPackage(p *packages.Package) (int, []finding) {
	files := 0
	var out []finding
	for _, f := range p.Syntax {
		if scan.IsTestFile(p.Fset.Position(f.Pos()).Filename) {
			continue
		}
		files++
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
				out = append(out, scanFunc(p, fd)...)
			}
		}
	}
	return files, out
}

// requireKnownHolders is the anti-vacuity check: every known holder must be among the findings.
func requireKnownHolders(out []finding) error {
	for _, want := range knownHolders {
		if !slices.ContainsFunc(out, func(f finding) bool { return f.fn == want }) {
			return fmt.Errorf("known holder %s not found: the scan is broken", want)
		}
	}
	return nil
}

// acquisition is one call that takes a lock or semaphore.
type acquisition struct {
	pos              token.Pos
	expr, recv, name string
}

// scanFunc finds, in source order, a lock acquisition followed later in the same function by
// calls whose method belongs to a type declared in the persistence packages.
func scanFunc(p *packages.Package, fd *ast.FuncDecl) []finding {
	fn := fd.Name.Name
	if fd.Recv != nil && len(fd.Recv.List) > 0 {
		fn = "(" + exprString(fd.Recv.List[0].Type) + ")." + fn
	}
	var out []finding
	for _, a := range acquisitions(fd) {
		releasePos := releaseOf(fd.Body, a.pos, a.recv, releaseFor[a.name])
		calls := persistenceCallsBetween(p, fd.Body, a.pos, releasePos)
		if len(calls) == 0 {
			continue
		}
		pos := scan.Pos(p.Fset.Position(a.pos))
		out = append(out, finding{pos: pos, fn: fn, lock: a.expr, calls: calls})
	}
	return out
}

// acquisitions lists the function's own, non-deferred lock and semaphore acquisitions; function
// literals inside it are not its own code path.
func acquisitions(fd *ast.FuncDecl) []acquisition {
	var acqs []acquisition
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		c, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := c.Fun.(*ast.SelectorExpr)
		if !ok || !acquireNames[sel.Sel.Name] {
			return true
		}
		if _, isDefer := parentDefer(fd.Body, c); !isDefer {
			acqs = append(acqs, acquisition{c.Pos(), exprString(sel), exprString(sel.X), sel.Sel.Name})
		}
		return true
	})
	return acqs
}

// persistenceCallsBetween lists, once each and in source order, the persistence I/O methods
// called in body between from and to.
func persistenceCallsBetween(p *packages.Package, body *ast.BlockStmt, from, to token.Pos) []string {
	seen := map[string]bool{}
	var calls []string
	ast.Inspect(body, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		c, ok := n.(*ast.CallExpr)
		if !ok || c.Pos() < from || c.Pos() > to {
			return true
		}
		if k, ok := persistenceIOMethod(p, c); ok && !seen[k] {
			seen[k] = true
			calls = append(calls, k)
		}
		return true
	})
	return calls
}

// persistenceIOMethod returns the method name when c calls a method of a persistence store,
// manager, queue or DB type; helpers and serializers in the same packages do not count.
func persistenceIOMethod(p *packages.Package, c *ast.CallExpr) (string, bool) {
	sel, ok := c.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	obj, ok := p.TypesInfo.Uses[sel.Sel].(*types.Func)
	if !ok || obj.Pkg() == nil || !strings.HasPrefix(obj.Pkg().Path(), persistencePrefix) {
		return "", false
	}
	sig, ok := obj.Type().(*types.Signature)
	if !ok || sig.Recv() == nil || !ioReceiver(sig.Recv().Type()) {
		return "", false
	}
	return obj.Name(), true
}

// releaseOf returns where the hold that starts at lockPos ends: the first matching release that
// is a statement in the same block as the acquisition (a release inside an early-return branch
// does not end it), or the end of the function (deferred release or none).
func releaseOf(body *ast.BlockStmt, lockPos token.Pos, recv, release string) token.Pos {
	end := body.End()
	ast.Inspect(body, func(n ast.Node) bool {
		b, ok := n.(*ast.BlockStmt)
		if !ok {
			return true
		}
		in := false
		for _, st := range b.List {
			if st.Pos() <= lockPos && lockPos < st.End() {
				in = in || startsHold(st, lockPos)
				continue
			}
			if in && isRelease(st, recv, release) {
				end = st.Pos()
				return false
			}
		}
		return !in
	})
	return end
}

// startsHold reports whether st is the statement that takes the lock at lockPos: the acquire call
// itself, or the init of an if (if err := acquire(); err != nil { ... }).
func startsHold(st ast.Stmt, lockPos token.Pos) bool {
	if es, ok := st.(*ast.ExprStmt); ok && es.X.Pos() == lockPos {
		return true
	}
	is, ok := st.(*ast.IfStmt)
	return ok && is.Init != nil && is.Init.Pos() <= lockPos && lockPos < is.Init.End()
}

// isRelease reports whether st is a call statement recv.release().
func isRelease(st ast.Stmt, recv, release string) bool {
	es, ok := st.(*ast.ExprStmt)
	if !ok {
		return false
	}
	c, ok := es.X.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := c.Fun.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == release && exprString(sel.X) == recv
}

func parentDefer(body *ast.BlockStmt, target ast.Node) (*ast.DeferStmt, bool) {
	var found *ast.DeferStmt
	ast.Inspect(body, func(n ast.Node) bool {
		if d, ok := n.(*ast.DeferStmt); ok && d.Call == target {
			found = d
		}
		return found == nil
	})
	return found, found != nil
}

func exprString(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return exprString(x.X) + "." + x.Sel.Name
	case *ast.StarExpr:
		return "*" + exprString(x.X)
	case *ast.CallExpr:
		return exprString(x.Fun) + "()"
	case *ast.IndexExpr:
		return exprString(x.X) + "[...]"
	}
	return "?"
}
