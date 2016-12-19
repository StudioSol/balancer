package balancer

import (
	"sort"
	"time"

	"github.com/StudioSol/balancer/concurrence"
)

type bySecondsBehindMaster Servers

func (a bySecondsBehindMaster) Len() int      { return len(a) }
func (a bySecondsBehindMaster) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a bySecondsBehindMaster) Less(i, j int) bool {
	if a[i].health.secondsBehindMaster == nil && a[j].health.secondsBehindMaster != nil {
		return false
	}
	if a[i].health.secondsBehindMaster != nil && a[j].health.secondsBehindMaster == nil {
		return true
	}
	return *a[i].health.secondsBehindMaster < *a[j].health.secondsBehindMaster
}

type byOpenConnections Servers

func (a byOpenConnections) Len() int      { return len(a) }
func (a byOpenConnections) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byOpenConnections) Less(i, j int) bool {
	if a[i].health.openConnections == nil && a[j].health.openConnections != nil {
		return false
	}
	if a[i].health.openConnections != nil && a[j].health.openConnections == nil {
		return true
	}
	return *a[i].health.openConnections < *a[j].health.openConnections
}

// Balancer MySQL load balancer
type Balancer struct {
	config  *Config
	servers Servers
	logger  Logger
	traceOn bool
}

// GetServers ...
func (b *Balancer) GetServers() Servers {
	return b.servers
}

// serversUP returns a slice of UP servers
func (b *Balancer) serversUP() Servers {
	serversUP := make(Servers, 0, len(b.servers))
	for _, server := range b.servers {
		if server.health.IsUP() {
			serversUP = append(serversUP, server)
		}
	}
	return serversUP
}

func (b *Balancer) startChecks() {
	for i := range b.servers {
		b.servers[i].CheckHealth(b.traceOn, b.logger)
	}
	concurrence.Every(time.Duration(b.config.CheckInterval)*time.Second, func(time.Time) bool {
		b.servers.eachASYNC(func(index int, server *Server) {
			server.CheckHealth(b.traceOn, b.logger)
		})
		return true
	})
}

// PickServer returns the best server at a given point in time
func (b *Balancer) PickServer() *Server {
	candidates := b.serversUP()
	switch len(candidates) {
	case 0:
		return nil
	case 1:
		return candidates[0]
	}

	candidates = candidates.filterBySecondsBehindMaster()
	switch len(candidates) {
	case 0:
		candidates = b.serversUP()
	case 1:
		return candidates[0]
	}

	sort.Sort(byOpenConnections(candidates))
	return candidates[0]
}

// New creates a new instance of Balancer
func New(config *Config) *Balancer {
	// Minimum check interval
	if config.CheckInterval == 0 {
		config.CheckInterval = 3
	}

	servers := make(Servers, len(config.ServersSettings))
	for i, serverSettings := range config.ServersSettings {
		servers[i] = &Server{
			name:           serverSettings.Name,
			serverSettings: serverSettings,
			health: &ServerHealth{
				lastUpdate: time.Now(),
			},
		}
	}

	balancer := &Balancer{
		config:  config,
		servers: servers,
		logger:  config.Logger,
		traceOn: config.TraceOn,
	}

	if config.StartCheck {
		balancer.startChecks()
	}

	return balancer
}

type Logger interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Printf(format string, v ...interface{})
}
