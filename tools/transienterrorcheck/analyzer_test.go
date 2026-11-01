package transienterrorcheck_test

import (
	"testing"

	"go.temporal.io/server/tools/transienterrorcheck"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), transienterrorcheck.Analyzer, "a")
}
