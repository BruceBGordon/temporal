package common

func IsResourceExhausted(err error) bool         { return false }
func IsPersistenceTransientError(err error) bool { return false }
