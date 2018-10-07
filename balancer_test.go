package balancer

import (
	"errors"
	"sort"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	ServerDownDueToMySQLConnection, ServerDownDueToMySQLSlaveStatus, ServerDownDueToMySQLThreadStatus         *Server
	ServerUP, ServerUPWithDelay, ServerUPWithHighThreadConnections, ServerUPWithDelayAndHighThreadConnections *Server
	ServerUPWithHighRunningConnections                                                                        *Server
)

func init() {
	var intNilHelper *int
	zeroHelper := 0
	oneHelper := 1
	thousandHelper := 1000

	ServerDownDueToMySQLConnection = &Server{
		name:   "ServerDownDueToMySQLConnection",
		health: &ServerHealth{},
	}
	ServerDownDueToMySQLConnection.health.setDown(
		errors.New("__MYSQL_CONNECTION_ERROR__"), intNilHelper, intNilHelper, intNilHelper,
	)

	ServerDownDueToMySQLSlaveStatus = &Server{
		name:   "ServerDownDueToMySQLSlaveStatus",
		health: &ServerHealth{},
	}
	ServerDownDueToMySQLSlaveStatus.health.setDown(
		errors.New("__MYSQL_SLAVE_STATUS_ERROR__"), intNilHelper, intNilHelper, intNilHelper,
	)

	ServerDownDueToMySQLThreadStatus = &Server{
		name:   "ServerDownDueToMySQLThreadStatus",
		health: &ServerHealth{},
	}
	ServerDownDueToMySQLThreadStatus.health.setDown(
		errors.New("__MYSQL_THREADS_STATUS_ERROR__"), &zeroHelper, intNilHelper, intNilHelper,
	)

	ServerUP = &Server{
		name:   "ServerUP",
		health: &ServerHealth{},
	}
	ServerUP.health.setUP(&zeroHelper, &oneHelper, &oneHelper)

	ServerUPWithDelay = &Server{
		name:   "ServerUPWithDelay",
		health: &ServerHealth{},
	}
	ServerUPWithDelay.health.setUP(&thousandHelper, &oneHelper, &oneHelper)

	ServerUPWithHighThreadConnections = &Server{
		name:   "ServerUPWithHighThreadConnections",
		health: &ServerHealth{},
	}
	ServerUPWithHighThreadConnections.health.setUP(&zeroHelper, &thousandHelper, &oneHelper)

	ServerUPWithDelayAndHighThreadConnections = &Server{
		name:   "ServerUPWithDelayAndHighThreadConnections",
		health: &ServerHealth{},
	}
	ServerUPWithDelayAndHighThreadConnections.health.setUP(&thousandHelper, &thousandHelper, &oneHelper)

	ServerUPWithHighRunningConnections = &Server{
		name:   "ServerUPWithHighRunningConnections",
		health: &ServerHealth{},
	}
	ServerUPWithHighRunningConnections.health.setUP(&zeroHelper, &thousandHelper, &thousandHelper)
}

func TestBalancer(t *testing.T) {
	defaultConfig := &Config{}

	Convey("Given a balancer with only one server", t, func() {
		Convey("It fails when the server is down due to error acquiring connection", func() {
			balancer := &Balancer{config: defaultConfig, servers: []*Server{
				ServerDownDueToMySQLConnection,
			}}
			So(balancer.PickServer(), ShouldBeNil)
		})

		Convey("It fails when the server is down due to error acquiring slave status", func() {
			balancer := &Balancer{config: defaultConfig, servers: []*Server{
				ServerDownDueToMySQLSlaveStatus,
			}}
			So(balancer.PickServer(), ShouldBeNil)
		})

		Convey("It fails when the server is down due to error acquiring thread status", func() {
			balancer := &Balancer{config: defaultConfig, servers: []*Server{
				ServerDownDueToMySQLThreadStatus,
			}}
			So(balancer.PickServer(), ShouldBeNil)
		})

		Convey("It succeeds when the server is healthy", func() {
			balancer := &Balancer{config: defaultConfig, servers: []*Server{
				ServerUP,
			}}
			So(balancer.PickServer(), ShouldPointTo, ServerUP)

			balancer = &Balancer{config: defaultConfig, servers: []*Server{
				ServerUPWithDelay,
			}}
			So(balancer.PickServer(), ShouldPointTo, ServerUPWithDelay)

			balancer = &Balancer{config: defaultConfig, servers: []*Server{
				ServerUPWithHighThreadConnections,
			}}
			So(balancer.PickServer(), ShouldPointTo, ServerUPWithHighThreadConnections)

			balancer = &Balancer{config: defaultConfig, servers: []*Server{
				ServerUPWithDelayAndHighThreadConnections,
			}}
			So(balancer.PickServer(), ShouldPointTo, ServerUPWithDelayAndHighThreadConnections)
		})
	})

	Convey("Given a balancer with more than one server", t, func() {
		Convey("It fails when all servers are down no matter the reason", func() {
			balancer := &Balancer{config: defaultConfig, servers: []*Server{
				ServerDownDueToMySQLConnection,
				ServerDownDueToMySQLSlaveStatus,
				ServerDownDueToMySQLThreadStatus,
			}}
			So(balancer.PickServer(), ShouldBeNil)

			balancer = &Balancer{config: defaultConfig, servers: []*Server{
				ServerDownDueToMySQLConnection,
				ServerDownDueToMySQLConnection,
				ServerDownDueToMySQLConnection,
			}}
			So(balancer.PickServer(), ShouldBeNil)

			balancer = &Balancer{config: defaultConfig, servers: []*Server{
				ServerDownDueToMySQLSlaveStatus,
				ServerDownDueToMySQLSlaveStatus,
				ServerDownDueToMySQLSlaveStatus,
			}}
			So(balancer.PickServer(), ShouldBeNil)

			balancer = &Balancer{config: defaultConfig, servers: []*Server{
				ServerDownDueToMySQLThreadStatus,
				ServerDownDueToMySQLThreadStatus,
				ServerDownDueToMySQLThreadStatus,
			}}
			So(balancer.PickServer(), ShouldBeNil)
		})

		Convey("In the case of one healthy slave", func() {

			Convey("In the case of one healthy slave", func() {
				Convey("It returns the healthy server no matter its index", func() {
					balancer := &Balancer{config: defaultConfig, servers: []*Server{
						ServerUP,
						ServerDownDueToMySQLConnection,
						ServerDownDueToMySQLConnection,
					}}
					So(balancer.PickServer(), ShouldPointTo, ServerUP)

					balancer = &Balancer{config: defaultConfig, servers: []*Server{
						ServerDownDueToMySQLConnection,
						ServerUP,
						ServerDownDueToMySQLConnection,
					}}
					So(balancer.PickServer(), ShouldPointTo, ServerUP)

					balancer = &Balancer{config: defaultConfig, servers: []*Server{
						ServerDownDueToMySQLConnection,
						ServerDownDueToMySQLConnection,
						ServerUP,
					}}
					So(balancer.PickServer(), ShouldPointTo, ServerUP)
				})
			})

			Convey("In the case of more than one healthy slaves", func() {
				Convey("It returns the healthyest server no matter its index", func() {
					ServerUP2 := *ServerUP

					balancer := &Balancer{config: defaultConfig, servers: []*Server{
						ServerUP,
						&ServerUP2,
						ServerDownDueToMySQLConnection,
						ServerUPWithDelay,
						ServerUPWithHighThreadConnections,
						ServerUPWithDelayAndHighThreadConnections,
					}}
					So(balancer.PickServer(), ShouldPointTo, ServerUP)

					balancer = &Balancer{config: defaultConfig, servers: []*Server{
						ServerDownDueToMySQLConnection,
						ServerUPWithDelay,
						ServerUPWithHighThreadConnections,
						&ServerUP2,
						ServerUP,
						ServerUPWithDelayAndHighThreadConnections,
					}}
					So(balancer.PickServer(), ShouldPointTo, &ServerUP2)

					balancer = &Balancer{config: defaultConfig, servers: []*Server{
						ServerDownDueToMySQLConnection,
						ServerUPWithDelay,
						ServerUPWithHighThreadConnections,
						ServerUPWithDelayAndHighThreadConnections,
						ServerUP,
						&ServerUP2,
					}}
					So(balancer.PickServer(), ShouldPointTo, ServerUP)

					balancer = &Balancer{config: defaultConfig, servers: []*Server{
						ServerDownDueToMySQLConnection,
						ServerUPWithDelayAndHighThreadConnections,
						ServerUPWithDelay,
						ServerUPWithHighThreadConnections,
					}}
					So(balancer.PickServer(), ShouldPointTo, ServerUPWithHighThreadConnections)

					balancer = &Balancer{config: defaultConfig, servers: []*Server{
						ServerDownDueToMySQLConnection,
						ServerUPWithDelayAndHighThreadConnections,
						ServerUPWithDelay,
					}}
					So(balancer.PickServer(), ShouldPointTo, ServerUPWithDelay)

					balancer = &Balancer{config: defaultConfig, servers: []*Server{
						ServerDownDueToMySQLConnection,
						ServerUPWithDelayAndHighThreadConnections,
					}}
					So(balancer.PickServer(), ShouldPointTo, ServerUPWithDelayAndHighThreadConnections)

					balancer = &Balancer{config: defaultConfig, servers: []*Server{
						ServerDownDueToMySQLConnection,
						ServerUPWithHighThreadConnections,
						ServerUPWithHighRunningConnections,
					}}
					So(balancer.PickServer(), ShouldPointTo, ServerUPWithHighThreadConnections)

				})
			})
		})
	})
}

func TestNewBalancer(t *testing.T) {
	Convey("When a valid config is given", t, func() {
		config := &Config{
			ServersSettings: []ServerSettings{
				{Name: "foo"},
				{Name: "bar"},
			},
			StartCheck: true,
		}

		Convey("It should return a valid balancer", func() {
			balancer := New(config)
			So(balancer, ShouldNotBeNil)
			So(balancer.GetServers(), ShouldHaveLength, 2)
		})
	})
}

func TestSortByConnection(t *testing.T) {
	Convey("When a list of servers are given", t, func() {
		servers := Servers{
			{name: "server_1", health: &ServerHealth{
				openConnections:    &[]int{3}[0],
				runningConnections: &[]int{2}[0],
			}},
			{name: "server_2", health: &ServerHealth{
				openConnections:    &[]int{3}[0],
				runningConnections: nil,
			}},
			{name: "server_3", health: &ServerHealth{
				openConnections:    nil,
				runningConnections: nil,
			}},
			{name: "server_4", health: &ServerHealth{
				openConnections:    &[]int{5}[0],
				runningConnections: &[]int{0}[0],
			}},
		}

		Convey("It should sort correctly", func() {
			sort.Sort(byConnections(servers))
			So(servers, ShouldHaveLength, 4)
			So(servers[0].name, ShouldEqual, "server_4")
			So(servers[1].name, ShouldEqual, "server_1")
			So(servers[2].name, ShouldEqual, "server_2")
			So(servers[3].name, ShouldEqual, "server_3")
		})
	})
}

func TestSortBySecondsBehindMaster(t *testing.T) {
	Convey("When a list of servers are given", t, func() {
		servers := Servers{
			{name: "server_2", health: &ServerHealth{
				secondsBehindMaster: &[]int{1}[0],
			}},
			{name: "server_1", health: &ServerHealth{
				secondsBehindMaster: nil,
			}},
			{name: "server_3", health: &ServerHealth{
				secondsBehindMaster: &[]int{1}[0],
			}},
			{name: "server_4", health: &ServerHealth{
				secondsBehindMaster: &[]int{0}[0],
			}},
		}

		Convey("It should sort correctly", func() {
			sort.Sort(bySecondsBehindMaster(servers))
			So(servers, ShouldHaveLength, 4)
			So(servers[0].name, ShouldEqual, "server_4")
			So(servers[1].name, ShouldEqual, "server_2")
			So(servers[2].name, ShouldEqual, "server_3")
			So(servers[3].name, ShouldEqual, "server_1")
		})
	})
}
