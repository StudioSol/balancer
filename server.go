package balancer

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-gorp/gorp"
)

// Server server representation
type Server struct {
	name                  string
	health                *ServerHealth
	serverSettings        ServerSettings
	connection            *gorp.DbMap
	replicationConnection *gorp.DbMap
	traceOn               bool
}

// GetName returns server's name
func (s *Server) GetName() string {
	return s.name
}

// GetHealth returns server's health state
func (s *Server) GetHealth() *ServerHealth {
	return s.health
}

// GetConnection returns server's connection
func (s *Server) GetConnection() *gorp.DbMap {
	return s.connection
}

func (s *Server) connect(dsn string, traceOn bool, logger Logger) (*gorp.DbMap, error) {
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	conn.SetMaxIdleConns(s.serverSettings.MaxIdleConns)
	conn.SetMaxOpenConns(s.serverSettings.MaxOpenConns)
	conn.SetConnMaxLifetime(s.serverSettings.MaxLifetimeConns)

	if err := conn.Ping(); err != nil {
		return nil, err
	}

	connection := &gorp.DbMap{
		Db:      conn,
		Dialect: gorp.MySQLDialect{},
	}

	if traceOn && logger != nil {
		connection.TraceOn("[sql]", logger)
	}

	return connection, nil
}

// CheckHealth check server's health and set it's state
func (s *Server) CheckHealth(traceOn bool, logger Logger) {
	var secondsBehindMaster, openConnections, runningConnections *int

	if err := s.connectIfNecessary(traceOn, logger); err != nil {
		s.health.setDown(
			err, secondsBehindMaster, openConnections, runningConnections,
		)
		return
	}

	slaveStatusResult, err := s.rawQuery("SHOW SLAVE STATUS", logger)
	if err == nil {
		rawSecondsBehindMaster := strings.TrimSpace(slaveStatusResult["Seconds_Behind_Master"])
		if rawSecondsBehindMaster == "" || strings.ToLower(rawSecondsBehindMaster) == "null" {
			s.health.setDown(
				fmt.Errorf("empty or null value for Seconds_Behind_Master returned from MySQL: %s", err),
				secondsBehindMaster, openConnections, runningConnections,
			)
			return
		}

		tmp, err := strconv.Atoi(rawSecondsBehindMaster)
		if err != nil {
			s.health.setDown(
				fmt.Errorf("unexpected value for Seconds_Behind_Master returned from MySQL (conversion error): %s", err),
				secondsBehindMaster, openConnections, runningConnections,
			)
			return
		}

		secondsBehindMaster = &tmp
	}

	threadsConnectedResult, err := s.rawQuery("SHOW STATUS LIKE 'Threads_connected'", logger)
	if err != nil {
		s.health.setDown(
			fmt.Errorf("failed acquiring MySQL thread connected status:  %s", err),
			secondsBehindMaster, openConnections, runningConnections,
		)
		return
	}

	threadsConnected := threadsConnectedResult["Value"]
	tmp2, err := strconv.Atoi(threadsConnected)
	if err != nil {
		s.health.setDown(
			fmt.Errorf("unexpected value for Threads_connected returned from MySQL:  %s", err),
			secondsBehindMaster, openConnections, runningConnections,
		)
		return
	}

	openConnections = &tmp2

	threadsRunningResult, err := s.rawQuery("SHOW STATUS LIKE 'Threads_running'", logger)
	if err != nil {
		s.health.setDown(
			fmt.Errorf("failed acquiring MySQL thread running status:  %s", err),
			secondsBehindMaster, openConnections, runningConnections,
		)
		return
	}

	threadsRunning := threadsRunningResult["Value"]
	tmp3, err := strconv.Atoi(threadsRunning)
	if err != nil {
		s.health.setDown(
			fmt.Errorf("unexpected value for Threads_running returned from MySQL:  %s", err),
			secondsBehindMaster, openConnections, runningConnections,
		)
		return
	}

	runningConnections = &tmp3

	s.health.setUP(secondsBehindMaster, openConnections, runningConnections)
}

func (s *Server) connectIfNecessary(traceOn bool, logger Logger) error {
	if s.connection == nil {
		conn, err := s.connect(s.serverSettings.DSN, traceOn, logger)
		if err != nil {
			return fmt.Errorf("could not connect to MySQL read user: %s", err.Error())
		}
		s.connection = conn
	}
	if s.replicationConnection == nil {
		conn, err := s.connect(s.serverSettings.ReplicationDSN, traceOn, logger)
		if err != nil {
			return fmt.Errorf("could not connect to MySQL replication user: %s", err.Error())
		}
		s.replicationConnection = conn
	}
	return nil
}

func (s *Server) rawQuery(query string, logger Logger) (map[string]string, error) {
	rows, err := s.replicationConnection.Db.Query(query)
	if err != nil {
		return nil, err
	}

	if !rows.Next() {
		return nil, sql.ErrNoRows
	}

	defer func() {
		if err := rows.Close(); err != nil && logger != nil {
			logger.Error(err)
		}
	}()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columns))
	for i := range values {
		var v sql.RawBytes
		values[i] = &v
	}

	err = rows.Scan(values...)
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for i, name := range columns {
		bp := values[i].(*sql.RawBytes)
		result[name] = string(*bp)
	}

	return result, nil
}
