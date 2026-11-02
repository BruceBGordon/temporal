// Package transienterrorcheck enforces that retry code treats the transient persistence error
// group declared by common.IsPersistenceTransientError as one group.
//
// common.IsPersistenceTransientError declares ResourceExhausted and Unavailable as one group.
// A function that branches on common.IsResourceExhausted, but never considers the rest of the
// group, usually gives overload protection (backoff, throttle, no immediate resubmit) to
// ResourceExhausted only. A brief Unavailable blip, such as a database failover, then bypasses
// that protection. That is one cause of https://github.com/temporalio/temporal/issues/11547.
//
// Calls are matched by resolved identity (package path and name), not by spelling, so aliases,
// renamed imports and same-named functions in other packages are handled correctly.
//
// A reviewed exception is written next to the code, with a reason:
//
//	//transienterror:ignore <reason>
//
// on the line of, or the line above, the flagged if statement. An exception without a reason,
// or one that no longer suppresses anything, is itself reported, so exceptions cannot rot.
package transienterrorcheck

import (
	"go/ast"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

const (
	commonPkg        = "go.temporal.io/server/common"
	serviceErrorPkg  = "go.temporal.io/api/serviceerror"
	directive        = "//transienterror:ignore"
	splitCheck       = "IsResourceExhausted"
	wholeGroupCheck  = "IsPersistenceTransientError"
	otherGroupMember = "Unavailable"
)

var Analyzer = &analysis.Analyzer{
	Name:     "transienterrorcheck",
	Doc:      "reports retry code that handles ResourceExhausted but not the rest of the transient persistence error group",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (any, error) {
	ins := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	type directiveInfo struct {
		pos  token.Pos
		used bool
	}
	directives := map[string]map[int]*directiveInfo{} // file -> line -> directive
	for _, f := range pass.Files {
		for _, group := range f.Comments {
			for _, c := range group.List {
				if !strings.HasPrefix(c.Text, directive) {
					continue
				}
				if strings.TrimSpace(strings.TrimPrefix(c.Text, directive)) == "" {
					pass.Reportf(c.Pos(), "%s needs a reason", directive)
				}
				p := pass.Fset.Position(c.Pos())
				if directives[p.Filename] == nil {
					directives[p.Filename] = map[int]*directiveInfo{}
				}
				directives[p.Filename][p.Line] = &directiveInfo{pos: c.Pos()}
			}
		}
	}
	exempted := func(pos token.Pos) bool {
		p := pass.Fset.Position(pos)
		for _, line := range []int{p.Line, p.Line - 1} {
			if d := directives[p.Filename][line]; d != nil {
				d.used = true
				return true
			}
		}
		return false
	}

	ins.Preorder([]ast.Node{(*ast.FuncDecl)(nil), (*ast.FuncLit)(nil)}, func(n ast.Node) {
		var body *ast.BlockStmt
		switch fn := n.(type) {
		case *ast.FuncDecl:
			body = fn.Body
		case *ast.FuncLit:
			body = fn.Body
		}
		if body == nil || coversWholeGroup(pass, body) {
			return
		}
		// The rule is about production retry paths; tests may deliberately retry on one error.
		if strings.HasSuffix(pass.Fset.Position(n.Pos()).Filename, "_test.go") {
			return
		}
		ast.Inspect(body, func(n ast.Node) bool {
			if _, ok := n.(*ast.FuncLit); ok {
				return false // checked on its own, with its own body
			}
			ifStmt, ok := n.(*ast.IfStmt)
			if !ok || !callsCommon(pass, ifStmt.Cond, splitCheck) {
				return true
			}
			if exempted(ifStmt.Pos()) {
				return true
			}
			effect := "branches on it"
			if computesTime(pass, ifStmt.Body) || (ifStmt.Else != nil && computesTime(pass, ifStmt.Else)) {
				effect = "chooses a delay with it"
			}
			pass.Reportf(ifStmt.Pos(),
				"%s %s but never considers %s (same group in common.%s): a blip can bypass overload protection",
				"common."+splitCheck, effect, "serviceerror."+otherGroupMember, wholeGroupCheck)
			return true
		})
	})

	for _, lines := range directives {
		for _, d := range lines {
			if !d.used {
				pass.Reportf(d.pos, "%s does not suppress anything; remove it", directive)
			}
		}
	}
	return nil, nil
}

// coversWholeGroup reports whether body calls common.IsPersistenceTransientError or refers to
// serviceerror.Unavailable, i.e. it considers the rest of the group.
func coversWholeGroup(pass *analysis.Pass, body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		if id, ok := n.(*ast.Ident); ok {
			obj := pass.TypesInfo.Uses[id]
			switch {
			case isObj(obj, commonPkg, wholeGroupCheck):
				found = true
			case isObj(obj, serviceErrorPkg, otherGroupMember):
				found = true
			}
		}
		return true
	})
	return found
}

// callsCommon reports whether expr contains a call to the function common.<name>, resolved by
// identity rather than by spelling.
func callsCommon(pass *analysis.Pass, expr ast.Expr, name string) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		var id *ast.Ident
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			id = fun
		case *ast.SelectorExpr:
			id = fun.Sel
		}
		if id != nil && isObj(pass.TypesInfo.Uses[id], commonPkg, name) {
			found = true
		}
		return true
	})
	return found
}

// computesTime reports whether n contains an expression of type time.Duration or time.Time,
// a type-based signal that the branch is choosing a delay.
func computesTime(pass *analysis.Pass, n ast.Node) bool {
	found := false
	ast.Inspect(n, func(n ast.Node) bool {
		if found {
			return false
		}
		if e, ok := n.(ast.Expr); ok {
			if named, ok := pass.TypesInfo.TypeOf(e).(*types.Named); ok {
				obj := named.Obj()
				if obj.Pkg() != nil && obj.Pkg().Path() == "time" && (obj.Name() == "Duration" || obj.Name() == "Time") {
					found = true
				}
			}
		}
		return true
	})
	return found
}

func isObj(obj types.Object, pkgPath, name string) bool {
	return obj != nil && obj.Pkg() != nil && obj.Pkg().Path() == pkgPath && obj.Name() == name
}
