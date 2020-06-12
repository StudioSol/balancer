package balancer

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-gorp/gorp"
	. "github.com/smartystreets/goconvey/convey"
)

type LoggerMock struct {
	ErrCalled map[error]bool
}

func (l *LoggerMock) Errorf(format string, args ...interface{}) {}
func (LoggerMock) Printf(format string, v ...interface{})       {}
func (l *LoggerMock) Error(args ...interface{}) {
	for _, err := range args {
		if err, ok := err.(error); ok {
			l.ErrCalled[err] = true
		}
	}
}

func newLoggerMock() *LoggerMock {
	return &LoggerMock{
		ErrCalled: make(map[error]bool),
	}
}

func getMock(t *testing.T) (*gorp.DbMap, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	dbMap := &gorp.DbMap{
		Db:      db,
		Dialect: gorp.MySQLDialect{"InnoDB", "UTF8"},
	}

	return dbMap, mock
}

func mockHealthQueries(t *testing.T, mock sqlmock.Sqlmock, ioStatus, secondsBehindMaster, openConnections, runningConnections driver.Value) {
	t.Helper()

	mock.ExpectQuery("SHOW STATUS LIKE 'Slave_running'").WillReturnRows(
		sqlmock.NewRows([]string{"Value"}).AddRow(ioStatus))

	mock.ExpectQuery("SHOW STATUS LIKE 'Threads_connected'").WillReturnRows(
		sqlmock.NewRows([]string{"Value"}).AddRow(openConnections))

	mock.ExpectQuery("SHOW STATUS LIKE 'Threads_running'").WillReturnRows(
		sqlmock.NewRows([]string{"Value"}).AddRow(runningConnections))

	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"Seconds_Behind_Master"}).AddRow(secondsBehindMaster))
}

func TestServerAttributes(t *testing.T) {
	Convey("When a valid server is given", t, func() {
		expectedHealth := new(ServerHealth)
		expectedConnections := new(gorp.DbMap)
		server := Server{
			name:       "server_name",
			health:     expectedHealth,
			connection: expectedConnections,
		}
		Convey("It should return the correct attributes", func() {
			So(server.GetName(), ShouldEqual, "server_name")
			So(server.GetConnection(), ShouldEqual, expectedConnections)
			So(server.GetHealth(), ShouldEqual, expectedHealth)
		})
	})
}

func TestRawQuery(t *testing.T) {
	Convey("Given a valid connection", t, func() {
		db, mock := getMock(t)
		server := Server{connection: db, replicationConnection: db}
		logger := newLoggerMock()

		Convey("When a valid query is given", func() {
			query := "SHOW SLAVE STATUS"

			mock.ExpectQuery(query).WillReturnRows(
				sqlmock.NewRows([]string{"Seconds_Behind_Master"}).AddRow(1))

			results, err := server.rawQuery(query, logger)

			Convey("It should return the expected result", func() {
				So(err, ShouldBeNil)
				So(results, ShouldHaveLength, 1)
				So(results["Seconds_Behind_Master"], ShouldEqual, "1")
				mock.ExpectationsWereMet()
			})
		})

		Convey("When an empty result is given", func() {
			query := "SHOW * FROM None"

			mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(
				sqlmock.NewRows([]string{"Foo"}))

			results, err := server.rawQuery(query, logger)

			Convey("It should fail with ErrNoRows", func() {
				So(err, ShouldEqual, sql.ErrNoRows)
				So(results, ShouldHaveLength, 0)
				mock.ExpectationsWereMet()
			})
		})

		Convey("When database connection fail", func() {
			query := "SHOW * FROM Fail"
			expectedError := errors.New("fail")

			mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnError(expectedError)
			results, err := server.rawQuery(query, logger)

			Convey("It should fail with expected error", func() {
				So(err, ShouldEqual, expectedError)
				So(results, ShouldHaveLength, 0)
				mock.ExpectationsWereMet()
			})
		})

		Convey("When rows fail on close", func() {
			query := "SHOW * FROM Fail"
			expectedError := errors.New("fail")

			mock.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(
				sqlmock.NewRows([]string{"Foo"}).
					AddRow(1).CloseError(expectedError))

			results, err := server.rawQuery(query, logger)

			Convey("It should fail with expected error", func() {
				So(err, ShouldBeNil)
				So(results, ShouldHaveLength, 1)
				So(logger.ErrCalled[expectedError], ShouldBeTrue)
				mock.ExpectationsWereMet()
			})
		})
	})
}

func TestCheckHealth(t *testing.T) {
	Convey("Given a valid server", t, func() {
		db, mock := getMock(t)
		logger := newLoggerMock()
		health := new(ServerHealth)
		server := Server{
			connection:            db,
			replicationConnection: db,
			health:                health,
		}

		Convey("When everything is ok", func() {
			mockHealthQueries(t, mock, "ON", 0, 2, 1)

			Convey("It should succeed without errors", func() {
				server.CheckHealth(false, logger)

				So(health.up, ShouldBeTrue)
				So(health.ioRunning, ShouldBeTrue)

				So(health.runningConnections, ShouldNotBeNil)
				So(*health.runningConnections, ShouldEqual, 1)

				So(health.openConnections, ShouldNotBeNil)
				So(*health.openConnections, ShouldEqual, 2)

				So(health.secondsBehindMaster, ShouldNotBeNil)
				So(*health.secondsBehindMaster, ShouldEqual, 0)

				So(mock.ExpectationsWereMet(), ShouldBeNil)
			})
		})

		Convey("When slave status are empty", func() {
			mockHealthQueries(t, mock, "ON", nil, 2, 1)

			Convey("It should set error on check", func() {
				server.CheckHealth(false, logger)

				So(health.up, ShouldBeTrue)
				So(health.err, ShouldNotBeNil)
				So(health.err.Error(), ShouldContainSubstring, "empty or null value for Seconds_Behind_Master")

				mock.ExpectationsWereMet()
			})
		})

		Convey("When openConnections are empty", func() {
			mockHealthQueries(t, mock, "ON", 0, nil, 1)

			Convey("It should set error on check", func() {
				server.CheckHealth(false, logger)

				So(health.up, ShouldBeTrue)
				So(health.err, ShouldNotBeNil)
				So(health.err.Error(), ShouldContainSubstring, "unexpected value for Threads_connected")

				mock.ExpectationsWereMet()
			})
		})

		Convey("When runningConnections are empty", func() {
			mockHealthQueries(t, mock, "ON", 0, 1, nil)

			Convey("It should set error on check", func() {
				server.CheckHealth(false, logger)
				So(health.up, ShouldBeTrue)
				So(health.err, ShouldNotBeNil)
				So(health.err.Error(), ShouldContainSubstring, "unexpected value for Threads_running")

				mock.ExpectationsWereMet()
			})
		})

		Convey("When IO is not running", func() {
			mockHealthQueries(t, mock, "OFF", 0, 1, 1)

			Convey("It should set io running false", func() {
				server.CheckHealth(false, logger)
				So(health.up, ShouldBeTrue)
				So(health.err, ShouldBeNil)
				So(health.ioRunning, ShouldBeFalse)

				mock.ExpectationsWereMet()
			})
		})
	})

	Convey("Given a invalid server", t, func() {
		logger := newLoggerMock()
		health := new(ServerHealth)
		server := Server{
			connection:            nil,
			replicationConnection: nil,
			health:                health,
		}

		Convey("When slave connection is nil", func() {

			Convey("It should fail with server down and errors", func() {
				server.CheckHealth(false, logger)

				So(health.up, ShouldBeFalse)
				So(health.err, ShouldNotBeNil)

			})
		})

	})

	Convey("Given a valid server without replication connection", t, func() {
		db, _ := getMock(t)
		logger := newLoggerMock()
		health := new(ServerHealth)
		server := Server{
			connection:            db,
			replicationConnection: nil,
			health:                health,
		}

		Convey("When slave connection is nil", func() {

			Convey("It should succeed with server down and errors", func() {
				server.CheckHealth(false, logger)

				So(health.up, ShouldBeTrue)
				So(health.err, ShouldNotBeNil)

			})
		})

	})

}
