package balancer

import "log"

// Config ...
type Config struct {
	CheckInterval int64
	StartCheck    bool
	TraceOn       bool
	Logger        *log.Logger
	Addresses     []Address
}

// Address ...
type Address struct {
	Name         string
	ConnString   string
	MaxIdleConns int
	MaxOpenConns int
}
