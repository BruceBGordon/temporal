// Command transienterrorcheck runs the transienterrorcheck analyzer, for use as
// go vet -vettool=$(which transienterrorcheck) ./...
package main

import (
	"go.temporal.io/server/tools/transienterrorcheck"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() {
	singlechecker.Main(transienterrorcheck.Analyzer)
}
