package balancer

import (
	"time"
)

type ReplicationMode int

const (
	ReplicationModeSingleSource ReplicationMode = iota
	ReplicationModeMultiSourceWriteSet
)

// Config configuration options for the balancer
type Config struct {
	CheckInterval   int64
	StartCheck      bool
	TraceOn         bool
	Logger          Logger
	ServersSettings []ServerSettings
	StartupWait     time.Duration
	ReplicationMode ReplicationMode
}

// ServerSettings servers' configuration options
type ServerSettings struct {
	Name             string
	DSN              string
	ReplicationDSN   string
	MaxIdleConns     int
	MaxOpenConns     int
	MaxLifetimeConns time.Duration
}
