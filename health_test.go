package balancer

import (
	"errors"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestHealthAttributes(t *testing.T) {
	Convey("When a valid health is given", t, func() {
		expectedErr := errors.New("fail")

		health := ServerHealth{
			err:                 expectedErr,
			openConnections:     &[]int{1}[0],
			runningConnections:  &[]int{2}[0],
			secondsBehindMaster: &[]int{3}[0],
			wsrepLocalState:     &[]int{4}[0],
			ioRunning:           true,
			wsrepReady:          true,
		}

		Convey("It should return correct values", func() {
			So(health.GetErr(), ShouldEqual, expectedErr)
			So(*health.GetOpenConnections(), ShouldEqual, 1)
			So(*health.GetRunningConnections(), ShouldEqual, 2)
			So(*health.GetSecondsBehindMaster(), ShouldEqual, 3)
			So(*health.GetWriteSetReplicationState(), ShouldEqual, 4)
			So(health.IORunning(), ShouldBeTrue)
			So(health.GetWriteSetReady(), ShouldBeTrue)
		})
	})
}
