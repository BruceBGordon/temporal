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
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

const persistencePrefix = "go.temporal.io/server/common/persistence"

func ioMethod(fn *types.Func) (string, bool) {
	sig, _ := fn.Type().(*types.Signature)
	if sig == nil || sig.Recv() == nil || sig.Params().Len() == 0 {
		return "", false
	}
	if t, ok := sig.Params().At(0).Type().(*types.Named); !ok || t.Obj().Name() != "Context" {
		return "", false
	}
	rt := sig.Recv().Type()
	if p, ok := rt.(*types.Pointer); ok {
		rt = p.Elem()
	}
	n, ok := rt.(*types.Named)
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
	}
	return "", false
}

// noDeadline reports whether e is a context with no deadline built in this function.
func noDeadline(e ast.Expr, vars map[string]bool) bool {
	switch x := e.(type) {
	case *ast.Ident:
		return vars[x.Name]
	case *ast.CallExpr:
		sel, ok := x.Fun.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		pkg, _ := sel.X.(*ast.Ident)
		switch {
		case pkg != nil && pkg.Name == "context" && (sel.Sel.Name == "Background" || sel.Sel.Name == "TODO"):
			return true
		case pkg != nil && pkg.Name == "context" && (sel.Sel.Name == "WithTimeout" || sel.Sel.Name == "WithDeadline" ||
			sel.Sel.Name == "WithTimeoutCause" || sel.Sel.Name == "WithDeadlineCause"):
			return false
		case len(x.Args) > 0:
			// WithCancel, WithValue, NewOutgoingContext, CopyContextValues...: inherit the deadline of
			// their first context argument.
			for _, a := range x.Args {
				if noDeadline(a, vars) {
					return true
				}
			}
		}
	}
	return false
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
	var out []string
	files := 0
	for _, p := range pkgs {
		if !strings.HasPrefix(p.PkgPath, "go.temporal.io/server") || strings.Contains(p.PkgPath, "/tools/") {
			continue
		}
		for _, f := range p.Syntax {
			name := p.Fset.Position(f.Pos()).Filename
			if strings.HasSuffix(name, "_test.go") || strings.Contains(name, "/tests/") || strings.Contains(name, "/testing/") ||
				strings.HasSuffix(name, "_gen.go") || strings.HasSuffix(name, ".pb.go") || strings.HasSuffix(name, "_mock.go") {
				continue
			}
			files++
			for _, d := range f.Decls {
				fd, ok := d.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				vars := map[string]bool{}
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					switch s := n.(type) {
					case *ast.AssignStmt:
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
					case *ast.CallExpr:
						sel, ok := s.Fun.(*ast.SelectorExpr)
						if !ok || len(s.Args) == 0 {
							return true
						}
						fn, ok := p.TypesInfo.Uses[sel.Sel].(*types.Func)
						if !ok {
							return true
						}
						kind, ok := ioMethod(fn)
						if !ok || !noDeadline(s.Args[0], vars) {
							return true
						}
						pos := p.Fset.Position(s.Pos())
						file := pos.Filename
						if i := strings.Index(file, "/temporal/"); i >= 0 {
							file = file[i+len("/temporal/"):]
						}
						out = append(out, fmt.Sprintf("%s:%d\t%s\t%s.%s", file, pos.Line, kind, recvName(fn), fn.Name()))
					}
					return true
				})
			}
		}
	}
	if files < 1000 {
		fmt.Fprintf(os.Stderr, "suspiciously little scanned: %d files\n", files)
		os.Exit(2)
	}
	sort.Strings(out)
	fmt.Printf("scanned %d files; %d persistence or RPC calls with a context that has no deadline\n\n", files, len(out))
	for _, o := range out {
		fmt.Println(o)
	}
}

func recvName(fn *types.Func) string {
	t := fn.Type().(*types.Signature).Recv().Type()
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	if n, ok := t.(*types.Named); ok {
		return n.Obj().Name()
	}
	return "?"
}
