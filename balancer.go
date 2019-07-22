package balancer

import (
	"sort"
	"sync/atomic"
	"time"

	"github.com/StudioSol/balancer/concurrence"
)

type bySecondsBehindMaster Servers

func (a bySecondsBehindMaster) Len() int      { return len(a) }
func (a bySecondsBehindMaster) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a bySecondsBehindMaster) Less(i, j int) bool {
	if a[i].health.secondsBehindMaster == nil && a[j].health.secondsBehindMaster == nil {
		return false
	}
	if a[i].health.secondsBehindMaster == nil && a[j].health.secondsBehindMaster != nil {
		return false
	}
	if a[i].health.secondsBehindMaster != nil && a[j].health.secondsBehindMaster == nil {
		return true
	}

	return *a[i].health.secondsBehindMaster < *a[j].health.secondsBehindMaster
}

type byConnections Servers

func (a byConnections) Len() int      { return len(a) }
func (a byConnections) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byConnections) Less(i, j int) bool {
	if a[i].health.runningConnections == nil && a[j].health.runningConnections != nil {
		return false
	}
	if a[i].health.runningConnections != nil && a[j].health.runningConnections == nil {
		return true
	}

	if a[i].health.runningConnections == a[j].health.runningConnections {

		if a[i].health.openConnections == nil && a[j].health.openConnections == nil {
			return false
		}
		if a[i].health.openConnections == nil && a[j].health.openConnections != nil {
			return false
		}
		if a[i].health.openConnections != nil && a[j].health.openConnections == nil {
			return true
		}
		return *a[i].health.openConnections < *a[j].health.openConnections

	}

	return *a[i].health.runningConnections < *a[j].health.runningConnections

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

func (b *Balancer) check() {
	b.servers.eachASYNC(func(index int, server *Server) {
		server.CheckHealth(b.traceOn, b.logger)
	})
}

func (b *Balancer) waitCheck() {
	wait := b.config.StartupWait
	// default to 5s
	if wait <= 0 {
		wait = time.Second * 5
	}

	signal := make(chan struct{}, 0)
	var expired atomic.Value

	t := time.AfterFunc(wait, func() {
		expired.Store(struct{}{})
		signal <- struct{}{}
	})

	go func() {
		for i := range b.servers {
			if expired.Load() != nil {
				return
			}
			b.servers[i].CheckHealth(b.traceOn, b.logger)
		}
		if t.Stop() {
			signal <- struct{}{}
		}
	}()

	<-signal
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

	sort.Sort(byConnections(candidates))
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

	balancer.waitCheck()
	if config.StartCheck {
		concurrence.Every(time.Duration(config.CheckInterval)*time.Second, func(time.Time) bool {
			balancer.check()
			return true
		})
	}

	return balancer
}

// Logger ...
type Logger interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Printf(format string, v ...interface{})
}
