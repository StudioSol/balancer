package balancer

import (
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestEachASYNC(t *testing.T) {
	Convey("When a list of functions are given", t, func() {
		servers := Servers([]*Server{{name: "foo"}, {name: "bar"}})
		called := make(map[string]bool)

		wg := new(sync.WaitGroup)
		wg.Add(2)

		mutex := new(sync.Mutex)
		function := func(v int, s *Server) {
			mutex.Lock()
			defer mutex.Unlock()

			called[s.name] = true
			wg.Done()
		}

		servers.eachASYNC(function)
		wg.Wait()

		Convey("all functions should be called", func() {
			for _, expectation := range called {
				So(expectation, ShouldBeTrue)
			}
		})
	})
}
