package balancer

import "github.com/go-gorp/gorp"

type Config struct {
	CheckInterval int64
	StartCheck    bool
	TraceOn       bool
	Logger        gorp.GorpLogger
	Addresses     []Address
}

type Address struct {
	Name         string
	ConnString   string `yaml:"conn_string"`
	MaxIdleConns int    `yaml:"max_idle_conns"`
	MaxOpenConns int    `yaml:"max_open_conns"`
}
