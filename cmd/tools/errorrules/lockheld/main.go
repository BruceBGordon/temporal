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
	"sort"
	"strings"

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

func main() {
	cfg := &packages.Config{
		Mode:       packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		BuildFlags: []string{"-tags=disable_grpc_modules"},
	}
	pkgs, err := packages.Load(cfg, os.Args[1:]...)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	bad := 0
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			bad++
			fmt.Fprintln(os.Stderr, "package error:", e)
		}
	})
	if bad > 0 {
		os.Exit(2)
	}
	var out []finding
	files := 0
	for _, p := range pkgs {
		if !strings.HasPrefix(p.PkgPath, "go.temporal.io/server") || strings.HasPrefix(p.PkgPath, persistencePrefix) {
			continue // the persistence layer itself is the callee, not a holder
		}
		for _, f := range p.Syntax {
			name := p.Fset.Position(f.Pos()).Filename
			if strings.HasSuffix(name, "_test.go") || strings.Contains(name, "/tests/") || strings.Contains(name, "/testing/") {
				continue
			}
			files++
			for _, d := range f.Decls {
				fd, ok := d.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				out = append(out, scan(p, fd)...)
			}
		}
	}
	if files < 1000 {
		fmt.Fprintf(os.Stderr, "suspiciously little scanned: %d files\n", files)
		os.Exit(2)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].pos < out[j].pos })
	// Anti-vacuity: the scan must find the holders behind #12246, or it is checking nothing.
	for _, want := range []string{"(*ContextImpl).AssertOwnership", "(*ContextImpl).UpdateWorkflowExecution", "(*ContextImpl).updateShardInfo"} {
		found := false
		for _, f := range out {
			found = found || f.fn == want
		}
		if !found {
			fmt.Fprintf(os.Stderr, "known holder %s not found: the scan is broken\n", want)
			os.Exit(2)
		}
	}
	fmt.Printf("scanned %d files; %d functions hold a lock across a persistence call\n\n", files, len(out))
	for _, f := range out {
		fmt.Printf("%s %s\n    holds %s across: %s\n", f.pos, f.fn, f.lock, strings.Join(f.calls, ", "))
	}
}

// scan finds, in source order, a lock acquisition followed later in the same function by calls
// whose method belongs to a type declared in the persistence packages.
func scan(p *packages.Package, fd *ast.FuncDecl) []finding {
	type acq struct {
		pos              token.Pos
		expr, recv, name string
	}
	var acqs []acq
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		if c, ok := n.(*ast.CallExpr); ok {
			if sel, ok := c.Fun.(*ast.SelectorExpr); ok && acquireNames[sel.Sel.Name] {
				if _, isDefer := parentDefer(fd.Body, c); !isDefer {
					acqs = append(acqs, acq{c.Pos(), exprString(sel), exprString(sel.X), sel.Sel.Name})
				}
			}
		}
		return true
	})
	fn := fd.Name.Name
	if fd.Recv != nil && len(fd.Recv.List) > 0 {
		fn = "(" + exprString(fd.Recv.List[0].Type) + ")." + fn
	}
	var out []finding
	for _, a := range acqs {
		releasePos := releaseOf(fd.Body, a.pos, a.recv, releaseFor[a.name])
		seen := map[string]bool{}
		var calls []string
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if _, ok := n.(*ast.FuncLit); ok {
				return false
			}
			c, ok := n.(*ast.CallExpr)
			if !ok || c.Pos() < a.pos || c.Pos() > releasePos {
				return true
			}
			sel, ok := c.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			obj, ok := p.TypesInfo.Uses[sel.Sel].(*types.Func)
			if !ok || obj.Pkg() == nil || !strings.HasPrefix(obj.Pkg().Path(), persistencePrefix) {
				return true
			}
			sig, _ := obj.Type().(*types.Signature)
			if sig == nil || sig.Recv() == nil || !ioReceiver(sig.Recv().Type()) {
				return true // a helper or serializer, not a call into a store, manager or queue
			}
			if k := obj.Name(); !seen[k] {
				seen[k] = true
				calls = append(calls, k)
			}
			return true
		})
		if len(calls) == 0 {
			continue
		}
		pos := p.Fset.Position(a.pos)
		file := pos.Filename
		if i := strings.Index(file, "/temporal/"); i >= 0 {
			file = file[i+len("/temporal/"):]
		}
		out = append(out, finding{pos: fmt.Sprintf("%s:%d", file, pos.Line), fn: fn, lock: a.expr, calls: calls})
	}
	return out
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
				if es, ok := st.(*ast.ExprStmt); ok && es.X.Pos() == lockPos {
					in = true
				} else if is, ok := st.(*ast.IfStmt); ok && is.Init != nil && is.Init.Pos() <= lockPos && lockPos < is.Init.End() {
					in = true // if err := acquire(); err != nil { ... }
				}
				continue
			}
			if !in {
				continue
			}
			if es, ok := st.(*ast.ExprStmt); ok {
				if c, ok := es.X.(*ast.CallExpr); ok {
					if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == release && exprString(sel.X) == recv {
						end = c.Pos()
						return false
					}
				}
			}
		}
		return !in
	})
	return end
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
