package balancer

import (
	"sync"
	"time"
)

type ServerHealth struct {
	sync.Mutex

	up         bool
	err        error
	lastUpdate time.Time

	secondsBehindMaster *int
	openConnections     *int
}

func (h *ServerHealth) IsUP() bool {
	h.Lock()
	defer h.Unlock()
	return h.up
}

func (h *ServerHealth) GetErr() error {
	return h.err
}

func (h *ServerHealth) GetSecondsBehindMaster() *int {
	return h.secondsBehindMaster
}

func (h *ServerHealth) GetOpenConnections() *int {
	return h.openConnections
}

func (h *ServerHealth) setUP(secondsBehindMaster *int, openConnections *int) {
	h.Lock()
	defer h.Unlock()
	h.up = true
	h.err = nil
	h.secondsBehindMaster = secondsBehindMaster
	h.openConnections = openConnections
	h.lastUpdate = time.Now()
}

func (h *ServerHealth) setDown(err error, secondsBehindMaster *int, openConnections *int) {
	h.Lock()
	defer h.Unlock()
	h.up = false
	h.err = err
	h.secondsBehindMaster = secondsBehindMaster
	h.openConnections = openConnections
	h.lastUpdate = time.Now()
}
