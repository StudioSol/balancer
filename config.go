package balancer

import "github.com/go-gorp/gorp"

type Config struct {
	CheckInterval int64
	StartCheck    bool
	TraceOn       bool
	Logger        gorp.GorpLogger
	DSNs          []DSN
}

type DSN struct {
	Name         string
	ConnString   string
	MaxIdleConns int
	MaxOpenConns int
}
