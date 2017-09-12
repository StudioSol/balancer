package balancer

import (
	"sync"
	"time"
)

// ServerHealth represents a Server health state
type ServerHealth struct {
	sync.Mutex

	up         bool
	err        error
	lastUpdate time.Time

	secondsBehindMaster *int
	openConnections     *int
	runningConnections  *int
}

// IsUP returns if the server is UP
func (h *ServerHealth) IsUP() bool {
	h.Lock()
	defer h.Unlock()
	return h.up
}

// GetErr returns server's last error
func (h *ServerHealth) GetErr() error {
	return h.err
}

// GetSecondsBehindMaster returns server's seconds behind master
func (h *ServerHealth) GetSecondsBehindMaster() *int {
	return h.secondsBehindMaster
}

// GetOpenConnections returns server's open connections
func (h *ServerHealth) GetOpenConnections() *int {
	return h.openConnections
}

// GetRunningConnections returns the number of connections that are not sleeping.
func (h *ServerHealth) GetRunningConnections() *int {
	return h.runningConnections
}

func (h *ServerHealth) setUP(secondsBehindMaster, openConnections, runningConnections *int) {
	h.Lock()
	defer h.Unlock()
	h.up = true
	h.err = nil
	h.secondsBehindMaster = secondsBehindMaster
	h.openConnections = openConnections
	h.runningConnections = runningConnections
	h.lastUpdate = time.Now()
}

func (h *ServerHealth) setDown(err error, secondsBehindMaster, openConnections, runningConnections *int) {
	h.Lock()
	defer h.Unlock()
	h.up = false
	h.err = err
	h.secondsBehindMaster = secondsBehindMaster
	h.openConnections = openConnections
	h.runningConnections = runningConnections
	h.lastUpdate = time.Now()
}
