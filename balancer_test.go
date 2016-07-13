package balancer

import (
	"errors"
	"testing"
	"time"

	"github.com/pborman/uuid"
	. "github.com/smartystreets/goconvey/convey"
)

func TestBalancer(t *testing.T) {
	var zero int
	one := 1
	ten := 10
	defaultConfig := &Config{}

	Convey("Given a balancer with only one server", t, func() {
		Convey("It fails when the server is down", func() {
			var servers Servers
			servers = append(servers, &Server{
				name: uuid.New(),
				health: &ServerHealth{
					up:                  false,
					err:                 errors.New("any error"),
					secondsBehindMaster: &zero,
					openConnections:     1,
					lastUpdate:          time.Now(),
				},
				config: defaultConfig,
			})

			balancer := &Balancer{config: defaultConfig, servers: servers}
			So(balancer.PickServer(), ShouldBeNil)
		})
		Convey("It succeeds when the server is UP", func() {
			var servers Servers
			servers = append(servers, &Server{
				name: "myserver",
				health: &ServerHealth{
					up:                  true,
					err:                 nil,
					secondsBehindMaster: &zero,
					openConnections:     1,
					lastUpdate:          time.Now(),
				},
				config: defaultConfig,
			})

			balancer := &Balancer{config: defaultConfig, servers: servers}
			So(balancer.PickServer().name, ShouldEqual, "myserver")
		})
	})

	Convey("Given a balancer with two or more servers", t, func() {
		Convey("It fails when all servers are down", func() {
			var servers Servers
			servers = append(servers, &Server{
				name: "myserver",
				health: &ServerHealth{
					up:                  false,
					err:                 errors.New("any error"),
					secondsBehindMaster: nil,
					openConnections:     0,
					lastUpdate:          time.Now(),
				},
				config: defaultConfig,
			})
			servers = append(servers, &Server{
				name: "myserver1",
				health: &ServerHealth{
					up:                  false,
					err:                 errors.New("any error"),
					secondsBehindMaster: nil,
					openConnections:     0,
					lastUpdate:          time.Now(),
				},
				config: defaultConfig,
			})
			servers = append(servers, &Server{
				name: "myserver2",
				health: &ServerHealth{
					up:                  false,
					err:                 errors.New("any error"),
					secondsBehindMaster: nil,
					openConnections:     0,
					lastUpdate:          time.Now(),
				},
				config: defaultConfig,
			})

			balancer := &Balancer{config: defaultConfig, servers: servers}
			So(balancer.PickServer(), ShouldBeNil)
		})

		Convey("When at least one server is up", func() {
			Convey("It should return the most suited server (up -> lowest seconds behind master -> less connected threads)", func() {
				var servers1 Servers
				servers1 = append(servers1, &Server{
					name: "myserver",
					health: &ServerHealth{
						up:                  false,
						err:                 errors.New("any error"),
						secondsBehindMaster: nil,
						openConnections:     0,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})
				servers1 = append(servers1, &Server{
					name: "myserver1",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: &zero,
						openConnections:     10,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})
				servers1 = append(servers1, &Server{
					name: "myserver2",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: &zero,
						openConnections:     1,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})

				balancer1 := &Balancer{config: defaultConfig, servers: servers1}
				So(balancer1.PickServer().name, ShouldEqual, "myserver2")

				var servers3 Servers
				servers3 = append(servers3, &Server{
					name: "myserver",
					health: &ServerHealth{
						up:                  false,
						err:                 errors.New("any error"),
						secondsBehindMaster: nil,
						openConnections:     0,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})
				servers3 = append(servers3, &Server{
					name: "myserver1",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: nil,
						openConnections:     10,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})
				servers3 = append(servers3, &Server{
					name: "myserver2",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: nil,
						openConnections:     1,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})

				balancer3 := &Balancer{config: defaultConfig, servers: servers3}
				So(balancer3.PickServer().name, ShouldEqual, "myserver2")

				var servers2 Servers
				servers2 = append(servers2, &Server{
					name: "myserver",
					health: &ServerHealth{
						up:                  false,
						err:                 errors.New("any error"),
						secondsBehindMaster: nil,
						openConnections:     0,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})
				servers2 = append(servers2, &Server{
					name: "myserver1",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: &one,
						openConnections:     10,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})
				servers2 = append(servers2, &Server{
					name: "myserver2",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: &ten,
						openConnections:     10,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})

				balancer2 := &Balancer{config: defaultConfig, servers: servers2}
				So(balancer2.PickServer().name, ShouldEqual, "myserver1")

				var servers4 Servers
				servers4 = append(servers4, &Server{
					name: "myserver",
					health: &ServerHealth{
						up:                  false,
						err:                 errors.New("any error"),
						secondsBehindMaster: nil,
						openConnections:     0,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})
				servers4 = append(servers4, &Server{
					name: "myserver1",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: &one,
						openConnections:     10,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})
				servers4 = append(servers4, &Server{
					name: "myserver2",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: &one,
						openConnections:     10,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})

				balancer4 := &Balancer{config: defaultConfig, servers: servers4}
				So(balancer4.PickServer().name, ShouldEqual, "myserver1")

				var servers5 Servers
				servers5 = append(servers5, &Server{
					name: "myserver",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: &zero,
						openConnections:     10,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})
				servers5 = append(servers5, &Server{
					name: "myserver1",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: &one,
						openConnections:     1,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})
				servers5 = append(servers5, &Server{
					name: "myserver2",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: nil,
						openConnections:     0,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})

				balancer5 := &Balancer{config: defaultConfig, servers: servers5}
				So(balancer5.PickServer().name, ShouldEqual, "myserver")

				var servers6 Servers
				servers6 = append(servers6, &Server{
					name: "myserver",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: nil,
						openConnections:     10,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})
				servers6 = append(servers6, &Server{
					name: "myserver1",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: &ten,
						openConnections:     999,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})
				servers6 = append(servers6, &Server{
					name: "myserver2",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: nil,
						openConnections:     0,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})

				balancer6 := &Balancer{config: defaultConfig, servers: servers6}
				So(balancer6.PickServer().name, ShouldEqual, "myserver1")

				var servers7 Servers
				servers7 = append(servers7, &Server{
					name: "myserver",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: &one,
						openConnections:     10,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})
				servers7 = append(servers7, &Server{
					name: "myserver1",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: &one,
						openConnections:     999,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})
				servers7 = append(servers7, &Server{
					name: "myserver2",
					health: &ServerHealth{
						up:                  true,
						err:                 nil,
						secondsBehindMaster: nil,
						openConnections:     0,
						lastUpdate:          time.Now(),
					},
					config: defaultConfig,
				})

				balancer7 := &Balancer{config: defaultConfig, servers: servers7}
				So(balancer7.PickServer().name, ShouldEqual, "myserver")
			})
		})
	})
}
