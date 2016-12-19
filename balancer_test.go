package balancer

import (
	"errors"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	ServerDownDueToMySQLConnection, ServerDownDueToMySQLSlaveStatus, ServerDownDueToMySQLThreadStatus         *Server
	ServerUP, ServerUPWithDelay, ServerUPWithHighThreadConnections, ServerUPWithDelayAndHighThreadConnections *Server
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
		errors.New("__MYSQL_CONNECTION_ERROR__"), intNilHelper, intNilHelper,
	)

	ServerDownDueToMySQLSlaveStatus = &Server{
		name:   "ServerDownDueToMySQLSlaveStatus",
		health: &ServerHealth{},
	}
	ServerDownDueToMySQLSlaveStatus.health.setDown(
		errors.New("__MYSQL_SLAVE_STATUS_ERROR__"), intNilHelper, intNilHelper,
	)

	ServerDownDueToMySQLThreadStatus = &Server{
		name:   "ServerDownDueToMySQLThreadStatus",
		health: &ServerHealth{},
	}
	ServerDownDueToMySQLThreadStatus.health.setDown(
		errors.New("__MYSQL_THREADS_STATUS_ERROR__"), &zeroHelper, intNilHelper,
	)

	ServerUP = &Server{
		name:   "ServerUP",
		health: &ServerHealth{},
	}
	ServerUP.health.setUP(&zeroHelper, &oneHelper)

	ServerUPWithDelay = &Server{
		name:   "ServerUPWithDelay",
		health: &ServerHealth{},
	}
	ServerUPWithDelay.health.setUP(&thousandHelper, &oneHelper)

	ServerUPWithHighThreadConnections = &Server{
		name:   "ServerUPWithHighThreadConnections",
		health: &ServerHealth{},
	}
	ServerUPWithHighThreadConnections.health.setUP(&zeroHelper, &thousandHelper)

	ServerUPWithDelayAndHighThreadConnections = &Server{
		name:   "ServerUPWithDelayAndHighThreadConnections",
		health: &ServerHealth{},
	}
	ServerUPWithDelayAndHighThreadConnections.health.setUP(&thousandHelper, &thousandHelper)
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
				})
			})
		})
	})
}
