// errorrules extracts every error-classification rule in the server (type
// switches over an error value) and analyzes them as a set to find gaps:
// error types that the code usually treats alike but some site splits.
//
// Usage: errorrules [-json out.json] [-focus T1,T2,...] ./...
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/printer"
	"go/token"
	"go/types"
	"os"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Site is one classification rule: a type switch over an error value.
type Site struct {
	Pos      string              `json:"pos"`
	Func     string              `json:"func"`
	Outcomes map[string][]string `json:"outcomes"` // outcome id -> error types
	Default  string              `json:"default"`  // outcome id for types not listed ("" if no default clause)
	Bodies   map[string]string   `json:"bodies"`   // outcome id -> normalized clause body
}

func (s *Site) outcomeOf(t string) (string, bool) {
	for o, ts := range s.Outcomes {
		for _, x := range ts {
			if x == t {
				return o, true
			}
		}
	}
	return s.Default, false
}

func main() {
	jsonOut := flag.String("json", "", "write all sites as JSON")
	focus := flag.String("focus", "", "comma-separated error types to tabulate across every site")
	minTogether := flag.Int("min", 3, "minimum sites where two types are treated alike before a split is reported")
	pairs := flag.Bool("pairs", false, "also report every implicit pair split (noisy)")
	flag.Parse()

	cfg := &packages.Config{
		Mode:       packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedDeps | packages.NeedImports,
		Tests:      false,
		BuildFlags: []string{"-tags=disable_grpc_modules"},
	}
	// Type-check from source (NeedDeps): golang.org/x/tools cannot read this Go version's export
	// data, and a package that fails to load must stop the run rather than be skipped.
	pkgs, err := packages.Load(cfg, flag.Args()...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load:", err)
		os.Exit(2)
	}
	// No silent fallbacks: any package that fails to load fails the run.
	loadErrs := 0
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			loadErrs++
			fmt.Fprintln(os.Stderr, "package error:", e)
		}
	})
	if loadErrs > 0 {
		os.Exit(2)
	}

	errorType := types.Universe.Lookup("error").Type()
	var sites []*Site
	files := 0
	for _, p := range pkgs {
		if !strings.HasPrefix(p.PkgPath, "go.temporal.io/server") {
			continue
		}
		for _, f := range p.Syntax {
			fname := p.Fset.Position(f.Pos()).Filename
			if strings.HasSuffix(fname, "_test.go") || strings.Contains(fname, "/testing/") || strings.Contains(fname, "/tests/") {
				continue
			}
			files++
			var curFunc string
			ast.Inspect(f, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.FuncDecl:
					curFunc = funcName(n)
				case *ast.TypeSwitchStmt:
					if s := extract(p, n, errorType, curFunc); s != nil {
						sites = append(sites, s)
					}
				}
				return true
			})
		}
	}
	// Anti-vacuity: the tool must have looked at the code it claims to cover.
	if files < 1000 || len(sites) < 20 {
		fmt.Fprintf(os.Stderr, "suspiciously little scanned: %d files, %d sites\n", files, len(sites))
		os.Exit(2)
	}
	fmt.Printf("scanned %d files, found %d error-classification sites\n\n", files, len(sites))

	if *jsonOut != "" {
		b, _ := json.MarshalIndent(sites, "", "  ")
		if err := os.WriteFile(*jsonOut, b, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
	if *focus != "" {
		tabulate(sites, strings.Split(*focus, ","))
	}
	preds := predicatesOf(sites)
	sameName(preds)
	restatements(sites, preds)
	if *pairs {
		splits(sites, *minTogether)
	}
}

// predicate is a declared classifier: a function whose type switch answers true or false.
type predicate struct {
	site  *Site
	name  string   // function name without receiver
	yes   []string // types listed with "return true"
	no    []string // types listed with "return false"
	deflt string   // "true", "false" or "" (no default clause)
}

func predicatesOf(sites []*Site) []predicate {
	var ps []predicate
	for _, s := range sites {
		p := predicate{site: s, name: s.Func[strings.LastIndex(s.Func, ".")+1:]}
		ok := true
		for o, ts := range s.Outcomes {
			switch s.Bodies[o] {
			case "return true;":
				p.yes = append(p.yes, ts...)
			case "return false;":
				p.no = append(p.no, ts...)
			default:
				ok = false
			}
		}
		switch s.Bodies[s.Default] {
		case "return true;":
			p.deflt = "true"
		case "return false;":
			p.deflt = "false"
		}
		if ok && len(p.yes)+len(p.no) > 0 {
			sort.Strings(p.yes)
			sort.Strings(p.no)
			ps = append(ps, p)
		}
	}
	return ps
}

// explicit is the list a predicate spells out (the side that is not the default).
func (p predicate) explicit() []string {
	if p.deflt == "true" {
		return p.no
	}
	return p.yes
}

func sameName(ps []predicate) {
	fmt.Println("== Declared classifiers that share a name but not a meaning ==")
	byName := map[string][]predicate{}
	for _, p := range ps {
		byName[p.name] = append(byName[p.name], p)
	}
	var names []string
	for n := range byName {
		names = append(names, n)
	}
	sort.Strings(names)
	found := 0
	for _, n := range names {
		g := byName[n]
		for i := 1; i < len(g); i++ {
			if strings.Join(g[0].yes, ",") != strings.Join(g[i].yes, ",") || g[0].deflt != g[i].deflt {
				found++
				fmt.Printf("  %s:\n    %s says true for %v\n    %s says true for %v\n", n, g[0].site.Pos, shorts(g[0].yes), g[i].site.Pos, shorts(g[i].yes))
			}
		}
	}
	if found == 0 {
		fmt.Println("  none")
	}
	fmt.Println()
}

// restatements finds inline type switches that repeat a declared classifier's list instead of
// calling it, and shows how each copy differs from the declaration.
func restatements(sites []*Site, ps []predicate) {
	fmt.Println("== Inline rules that restate a declared classifier (copies drift) ==")
	found := 0
	for _, p := range ps {
		decl := p.explicit()
		if len(decl) < 3 {
			continue
		}
		for _, s := range sites {
			if s == p.site {
				continue
			}
			for o, ts := range s.Outcomes {
				missing, extra := diff(decl, ts)
				common := len(decl) - len(missing)
				if common < 3 || float64(common)/float64(len(decl)+len(extra)) < 0.6 {
					continue
				}
				found++
				status := "identical"
				if len(missing)+len(extra) > 0 {
					status = fmt.Sprintf("DIFFERS: missing %v, extra %v", shorts(missing), shorts(extra))
				}
				fmt.Printf("  %s %s restates %s (%s) in one branch [%s]: %s\n",
					s.Pos, s.Func, p.site.Func, p.site.Pos, trunc(s.Bodies[o], 50), status)
			}
		}
	}
	if found == 0 {
		fmt.Println("  none")
	}
	fmt.Println()
}

func diff(want, got []string) (missing, extra []string) {
	in := func(xs []string, x string) bool {
		for _, y := range xs {
			if y == x {
				return true
			}
		}
		return false
	}
	for _, w := range want {
		if !in(got, w) {
			missing = append(missing, w)
		}
	}
	for _, g := range got {
		if !in(want, g) {
			extra = append(extra, g)
		}
	}
	return
}

func shorts(ts []string) []string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = short(t)
	}
	return out
}

func funcName(fd *ast.FuncDecl) string {
	if fd.Recv != nil && len(fd.Recv.List) > 0 {
		var b bytes.Buffer
		_ = printer.Fprint(&b, token.NewFileSet(), fd.Recv.List[0].Type)
		return "(" + b.String() + ")." + fd.Name.Name
	}
	return fd.Name.Name
}

func extract(p *packages.Package, ts *ast.TypeSwitchStmt, errorType types.Type, fn string) *Site {
	var x ast.Expr
	switch a := ts.Assign.(type) {
	case *ast.AssignStmt:
		x = a.Rhs[0].(*ast.TypeAssertExpr).X
	case *ast.ExprStmt:
		x = a.X.(*ast.TypeAssertExpr).X
	}
	tv, ok := p.TypesInfo.Types[x]
	if !ok || !types.Identical(tv.Type, errorType) {
		return nil
	}
	s := &Site{
		Pos:      shortPos(p.Fset.Position(ts.Pos())),
		Func:     fn,
		Outcomes: map[string][]string{},
		Bodies:   map[string]string{},
	}
	bodyIDs := map[string]string{} // normalized body -> outcome id
	for _, c := range ts.Body.List {
		cc := c.(*ast.CaseClause)
		body := normalize(cc.Body)
		id, ok := bodyIDs[body]
		if !ok {
			id = fmt.Sprintf("o%d", len(bodyIDs)+1)
			bodyIDs[body] = id
			s.Bodies[id] = body
		}
		if cc.List == nil {
			s.Default = id
			continue
		}
		for _, e := range cc.List {
			t := p.TypesInfo.Types[e].Type
			if t == nil {
				continue
			}
			s.Outcomes[id] = append(s.Outcomes[id], typeName(t))
		}
	}
	if len(s.Outcomes) == 0 {
		return nil
	}
	return s
}

// normalize prints a clause body without comments so that clauses that do
// the same thing compare equal.
func normalize(stmts []ast.Stmt) string {
	var b bytes.Buffer
	for _, st := range stmts {
		_ = printer.Fprint(&b, token.NewFileSet(), st)
		b.WriteString(";")
	}
	return strings.Join(strings.Fields(b.String()), " ")
}

func typeName(t types.Type) string {
	if t == nil {
		return "nil"
	}
	s := types.TypeString(t, func(p *types.Package) string { return p.Name() })
	return strings.TrimPrefix(s, "*")
}

func shortPos(p token.Position) string {
	f := p.Filename
	if i := strings.Index(f, "/temporal/"); i >= 0 {
		f = f[i+len("/temporal/"):]
	}
	return fmt.Sprintf("%s:%d", f, p.Line)
}

func tabulate(sites []*Site, focus []string) {
	fmt.Println("== How each site treats the focus types (same letter = same outcome at that site; '-' = default, '.' = no default) ==")
	fmt.Printf("%-70s", "site")
	for _, t := range focus {
		fmt.Printf(" %-22s", t)
	}
	fmt.Println()
	for _, s := range sites {
		mentioned := false
		for _, t := range focus {
			if _, ok := s.outcomeOf(t); ok {
				mentioned = true
			}
		}
		if !mentioned {
			continue
		}
		letters := map[string]string{}
		fmt.Printf("%-70s", trunc(s.Pos+" "+s.Func, 70))
		for _, t := range focus {
			o, explicit := s.outcomeOf(t)
			cell := "."
			if o != "" {
				if _, ok := letters[o]; !ok {
					letters[o] = string(rune('A' + len(letters)))
				}
				cell = letters[o]
				if !explicit {
					cell += " (default)"
				}
			}
			fmt.Printf(" %-22s", cell)
		}
		fmt.Println()
	}
	fmt.Println()
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

// splits finds implicit groups: pairs of error types that several sites treat
// identically (listed together, same outcome), and reports the sites that
// treat the pair differently.
func splits(sites []*Site, minTogether int) {
	type pair struct{ a, b string }
	together := map[pair][]*Site{}
	apart := map[pair][]*Site{}
	for _, s := range sites {
		var listed []string
		for _, ts := range s.Outcomes {
			listed = append(listed, ts...)
		}
		sort.Strings(listed)
		for i := range listed {
			for j := i + 1; j < len(listed); j++ {
				a, b := listed[i], listed[j]
				if a == b {
					continue
				}
				oa, _ := s.outcomeOf(a)
				ob, _ := s.outcomeOf(b)
				if oa == ob {
					together[pair{a, b}] = append(together[pair{a, b}], s)
				}
			}
		}
	}
	// A split: a site that mentions one member explicitly and gives the
	// other a different outcome (explicitly or through default).
	for pr, ts := range together {
		if len(ts) < minTogether {
			continue
		}
		for _, s := range sites {
			oa, ea := s.outcomeOf(pr.a)
			ob, eb := s.outcomeOf(pr.b)
			if !ea && !eb {
				continue
			}
			if oa != ob {
				apart[pr] = append(apart[pr], s)
			}
		}
	}
	var prs []pair
	for pr := range apart {
		prs = append(prs, pr)
	}
	sort.Slice(prs, func(i, j int) bool {
		if len(together[prs[i]]) != len(together[prs[j]]) {
			return len(together[prs[i]]) > len(together[prs[j]])
		}
		return prs[i].a+prs[i].b < prs[j].a+prs[j].b
	})
	fmt.Printf("== Implicit groups that some site splits (treated alike at >= %d sites) ==\n", minTogether)
	for _, pr := range prs {
		fmt.Printf("\n%s + %s: alike at %d sites, split at %d:\n", pr.a, pr.b, len(together[pr]), len(apart[pr]))
		for _, s := range apart[pr] {
			oa, ea := s.outcomeOf(pr.a)
			ob, eb := s.outcomeOf(pr.b)
			fmt.Printf("    %s %s: %s->%s%s, %s->%s%s\n", s.Pos, s.Func, short(pr.a), oa, dflt(ea), short(pr.b), ob, dflt(eb))
		}
	}
}

func short(t string) string { return t[strings.LastIndex(t, ".")+1:] }
func dflt(explicit bool) string {
	if explicit {
		return ""
	}
	return "(default)"
}
