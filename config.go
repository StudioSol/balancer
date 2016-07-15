package balancer

type Config struct {
	CheckInterval   int64
	StartCheck      bool
	TraceOn         bool
	Logger          Logger
	ServersSettings []ServerSettings
}

type ServerSettings struct {
	Name         string
	DSN          string
	MaxIdleConns int
	MaxOpenConns int
}
