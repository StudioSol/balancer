package balancer

import "github.com/go-gorp/gorp"

type Config struct {
	CheckInterval   int64
	StartCheck      bool
	TraceOn         bool
	Logger          gorp.GorpLogger
	ServersSettings []ServerSettings
}

type ServerSettings struct {
	Name         string
	DSN          string
	MaxIdleConns int
	MaxOpenConns int
}
