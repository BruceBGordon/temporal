package serviceerror

type Unavailable struct{}

func (*Unavailable) Error() string { return "unavailable" }
