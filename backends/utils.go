package backends

// ForEachFn is called for ForEach methods
type ForEachFn func(key string) (err error)
