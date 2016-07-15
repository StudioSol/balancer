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
	return *a[i].health.secondsBehindMaster < *a[j].health.secondsBehindMaster
}

type byOpenConnections Servers

func (a byOpenConnections) Len() int      { return len(a) }
func (a byOpenConnections) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byOpenConnections) Less(i, j int) bool {
	return a[i].health.openConnections < a[j].health.openConnections
}

// Balancer MySQL load balancer
type Balancer struct {
	config  *Config
	servers Servers
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
		b.servers[i].CheckHealth()
	}
	if b.config.CheckInterval == 0 {
		b.config.CheckInterval = 3
	}
	concurrence.Every(time.Duration(b.config.CheckInterval)*time.Second, func(time.Time) bool {
		b.servers.eachASYNC(func(index int, server *Server) {
			server.CheckHealth()
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
	servers := make(Servers, len(config.DSNs))
	for i, DSN := range config.DSNs {
		servers[i] = &Server{
			name: DSN.Name,
			dsns: DSN,
			health: &ServerHealth{
				up:         false,
				err:        nil,
				lastUpdate: time.Now(),
			},
			config: config,
		}
	}

	balancer := &Balancer{
		config:  config,
		servers: servers,
	}

	if config.StartCheck {
		balancer.startChecks()
	}

	return balancer
}
