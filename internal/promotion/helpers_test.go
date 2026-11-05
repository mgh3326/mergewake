package promotion

import "fmt"

func sscan(s string, a ...any) (int, error) { return fmt.Sscan(s, a...) }
