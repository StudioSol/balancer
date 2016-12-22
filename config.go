package balancer

// Config configuration options for the balancer
type Config struct {
	CheckInterval   int64
	StartCheck      bool
	TraceOn         bool
	Logger          Logger
	ServersSettings []ServerSettings
}

// ServerSettings servers' configuration options
type ServerSettings struct {
	Name           string
	DSN            string
	ReplicationDSN string
	MaxIdleConns   int
	MaxOpenConns   int
}
