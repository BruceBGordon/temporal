package persistence

// IsUnhealthyErrorForTest exposes the health signal's classifier to the external test package
// that checks the error rules against each other (error_rules_test.go).
var IsUnhealthyErrorForTest = isUnhealthyError
