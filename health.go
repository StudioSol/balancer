package balancer

import (
	"sync"
	"time"
)

type ServerHealth struct {
	up                  bool
	err                 error
	secondsBehindMaster *int
	openConnections     int
	lastUpdate          time.Time
	mutex               sync.Mutex
}

func (h *ServerHealth) IsUP() bool {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.up
}

func (h *ServerHealth) setUP() {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.up = true
	h.err = nil
	h.lastUpdate = time.Now()
}

func (h *ServerHealth) setDown(err error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.up = false
	h.err = err
	h.lastUpdate = time.Now()
}

func (h *ServerHealth) GetErr() error {
	return h.err
}

func (h *ServerHealth) GetSecondsBehindMaster() *int {
	return h.secondsBehindMaster
}

func (h *ServerHealth) GetOpenConnections() int {
	return h.openConnections
}
