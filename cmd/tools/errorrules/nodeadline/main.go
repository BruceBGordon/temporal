// nodeadline lists persistence and service-RPC calls made with a context that carries no
// deadline: one built in the same function from context.Background() or context.TODO() through
// WithCancel, WithValue and similar, never WithTimeout or WithDeadline. Such a call is bounded only
// by whatever the layers below enforce (a driver, the network, the OS). Intra-procedural:
// contexts passed in as parameters or stored in fields are not judged. Run from the repo root.
package main

import (
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"slices"
	"strings"

	"go.temporal.io/server/cmd/tools/errorrules/internal/scan"
	"golang.org/x/tools/go/packages"
)

const persistencePrefix = "go.temporal.io/server/common/persistence"

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
	var out []string
	files := 0
	for _, p := range pkgs {
		if !strings.HasPrefix(p.PkgPath, "go.temporal.io/server") || strings.Contains(p.PkgPath, "/tools/") {
			continue
		}
		n, found := scanPackage(p)
		files += n
		out = append(out, found...)
	}
	if err := scan.RequireScanned(files); err != nil {
		return err
	}
	slices.Sort(out)
	fmt.Printf("scanned %d files; %d persistence or RPC calls with a context that has no deadline\n\n", files, len(out))
	for _, o := range out {
		fmt.Println(o)
	}
	return nil
}

// scanPackage scans the package's production files and returns how many it scanned and the
// calls it found.
func scanPackage(p *packages.Package) (int, []string) {
	files := 0
	var out []string
	for _, f := range p.Syntax {
		name := p.Fset.Position(f.Pos()).Filename
		if scan.IsTestFile(name) || isGenerated(name) {
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

func isGenerated(name string) bool {
	return strings.HasSuffix(name, "_gen.go") || strings.HasSuffix(name, ".pb.go") || strings.HasSuffix(name, "_mock.go")
}

// scanFunc walks one function in source order, tracking which local variables hold a context
// with no deadline, and reports each I/O call made with one.
func scanFunc(p *packages.Package, fd *ast.FuncDecl) []string {
	vars := map[string]bool{} // local name -> holds a context with no deadline
	var out []string
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.AssignStmt:
			trackAssign(s, vars)
		case *ast.CallExpr:
			if found, ok := checkCall(p, s, vars); ok {
				out = append(out, found)
			}
		default:
			// other nodes neither bind a context nor make a call
		}
		return true
	})
	return out
}

// trackAssign records whether each variable assigned by s now holds a context with no deadline.
func trackAssign(s *ast.AssignStmt, vars map[string]bool) {
	for i, l := range s.Lhs {
		id, ok := l.(*ast.Ident)
		if !ok {
			continue
		}
		r := s.Rhs[0]
		if len(s.Rhs) == len(s.Lhs) {
			r = s.Rhs[i]
		} else if i != 0 {
			continue // ctx, cancel := context.WithCancel(...): only the first result
		}
		if noDeadline(r, vars) {
			vars[id.Name] = true
		} else if _, isCall := r.(*ast.CallExpr); isCall || len(s.Rhs) == len(s.Lhs) {
			delete(vars, id.Name)
		}
	}
}

// checkCall reports a persistence or RPC call whose context argument has no deadline.
func checkCall(p *packages.Package, s *ast.CallExpr, vars map[string]bool) (string, bool) {
	sel, ok := s.Fun.(*ast.SelectorExpr)
	if !ok || len(s.Args) == 0 {
		return "", false
	}
	fn, ok := p.TypesInfo.Uses[sel.Sel].(*types.Func)
	if !ok {
		return "", false
	}
	kind, ok := ioMethod(fn)
	if !ok || !noDeadline(s.Args[0], vars) {
		return "", false
	}
	pos := scan.Pos(p.Fset.Position(s.Pos()))
	return fmt.Sprintf("%s\t%s\t%s.%s", pos, kind, recvName(fn), fn.Name()), true
}

// ioMethod reports whether fn is a method that does I/O and takes a context first: a method on a
// persistence manager, store or queue, or on a service RPC client.
func ioMethod(fn *types.Func) (string, bool) {
	sig, ok := fn.Type().(*types.Signature)
	if !ok || sig.Recv() == nil || sig.Params().Len() == 0 {
		return "", false
	}
	if t, ok := sig.Params().At(0).Type().(*types.Named); !ok || t.Obj().Name() != "Context" {
		return "", false
	}
	n, ok := namedRecv(sig)
	if !ok || n.Obj().Pkg() == nil {
		return "", false
	}
	pkg, name := n.Obj().Pkg().Path(), n.Obj().Name()
	switch {
	case strings.HasPrefix(pkg, persistencePrefix) &&
		(strings.HasSuffix(name, "Manager") || strings.HasSuffix(name, "Store") || strings.HasSuffix(name, "Queue")):
		return "persistence", true
	case strings.HasSuffix(name, "ServiceClient"):
		return "rpc", true
	default:
		return "", false
	}
}

// noDeadline reports whether e is a context with no deadline built in this function.
func noDeadline(e ast.Expr, vars map[string]bool) bool {
	switch x := e.(type) {
	case *ast.Ident:
		return vars[x.Name]
	case *ast.CallExpr:
		return callHasNoDeadline(x, vars)
	default:
		return false
	}
}

// callHasNoDeadline reports whether a call builds a context with no deadline: context.Background
// or TODO, or a wrapper over a context that has none. WithTimeout and WithDeadline add one.
func callHasNoDeadline(x *ast.CallExpr, vars map[string]bool) bool {
	sel, ok := x.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, isIdent := sel.X.(*ast.Ident)
	fromContext := isIdent && pkg.Name == "context"
	switch {
	case fromContext && (sel.Sel.Name == "Background" || sel.Sel.Name == "TODO"):
		return true
	case fromContext && (sel.Sel.Name == "WithTimeout" || sel.Sel.Name == "WithDeadline" ||
		sel.Sel.Name == "WithTimeoutCause" || sel.Sel.Name == "WithDeadlineCause"):
		return false
	default:
		// WithCancel, WithValue, NewOutgoingContext, CopyContextValues...: inherit the deadline of
		// their context argument.
		return slices.ContainsFunc(x.Args, func(a ast.Expr) bool { return noDeadline(a, vars) })
	}
}

// namedRecv returns the named type of a method's receiver, through a pointer.
func namedRecv(sig *types.Signature) (*types.Named, bool) {
	t := sig.Recv().Type()
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	n, ok := t.(*types.Named)
	return n, ok
}

func recvName(fn *types.Func) string {
	sig, ok := fn.Type().(*types.Signature)
	if !ok || sig.Recv() == nil {
		return "?"
	}
	if n, ok := namedRecv(sig); ok {
		return n.Obj().Name()
	}
	return "?"
}
