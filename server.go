package balancer

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-gorp/gorp/v3"
)

var mutex sync.Mutex

// Server server representation
type Server struct {
	name                  string
	health                *ServerHealth
	serverSettings        ServerSettings
	connection            *gorp.DbMap
	replicationConnection *gorp.DbMap
	traceOn               bool
	isChecking            int32
	replicationMode       ReplicationMode
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
		conn.Close()
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
	var secondsBehindMaster, openConnections, runningConnections, wsrepLocalState *int

	// prevent concurrently checks on same server (slow queries/network)
	if atomic.LoadInt32(&s.isChecking) == 1 {
		return
	}

	atomic.StoreInt32(&s.isChecking, 1)
	defer func() {
		atomic.StoreInt32(&s.isChecking, 0)
	}()

	if err := s.connectReadUser(traceOn, logger); err != nil {
		s.health.setDown(
			err, false, false, secondsBehindMaster, openConnections, runningConnections, wsrepLocalState,
		)
		return
	}

	if err := s.connectReplicationUser(traceOn, logger); err != nil {
		s.health.setUP(
			err, false, false, secondsBehindMaster, openConnections, runningConnections, wsrepLocalState,
		)
		return
	}

	ioRunning := false
	wsrepReady := false
	if s.replicationMode == ReplicationModeSingleSource {
		ioRunningResult, err := s.rawQuery("SHOW STATUS LIKE 'Slave_running'", logger)
		if err == nil && strings.EqualFold(ioRunningResult["Value"], "ON") {
			ioRunning = true
		}
	} else if s.replicationMode == ReplicationModeMultiSourceWriteSet {
		ioRunningResult, err := s.rawQuery("SHOW STATUS LIKE 'wsrep_connected'", logger)
		if err == nil && strings.EqualFold(ioRunningResult["Value"], "ON") {
			ioRunning = true
		}
		readyResult, err := s.rawQuery("SHOW STATUS LIKE 'wsrep_ready'", logger)
		if err == nil && strings.EqualFold(readyResult["Value"], "ON") {
			wsrepReady = true
		}
	}

	threadsConnectedResult, err := s.rawQuery("SHOW STATUS LIKE 'Threads_connected'", logger)
	if err != nil {
		s.health.setUP(
			fmt.Errorf("failed acquiring MySQL thread connected status:  %s", err),
			ioRunning, wsrepReady, secondsBehindMaster, openConnections, runningConnections, wsrepLocalState,
		)
		return
	}

	threadsConnected := threadsConnectedResult["Value"]
	tmp2, err := strconv.Atoi(threadsConnected)
	if err != nil {
		s.health.setUP(
			fmt.Errorf("unexpected value for Threads_connected returned from MySQL:  %s", err),
			ioRunning, wsrepReady, secondsBehindMaster, openConnections, runningConnections, wsrepLocalState,
		)
		return
	}

	openConnections = &tmp2

	threadsRunningResult, err := s.rawQuery("SHOW STATUS LIKE 'Threads_running'", logger)
	if err != nil {
		s.health.setUP(
			fmt.Errorf("failed acquiring MySQL thread running status:  %s", err),
			ioRunning, wsrepReady, secondsBehindMaster, openConnections, runningConnections, wsrepLocalState,
		)
		return
	}

	threadsRunning := threadsRunningResult["Value"]
	tmp3, err := strconv.Atoi(threadsRunning)
	if err != nil {
		s.health.setUP(
			fmt.Errorf("unexpected value for Threads_running returned from MySQL:  %s", err),
			ioRunning, wsrepReady, secondsBehindMaster, openConnections, runningConnections, wsrepLocalState,
		)
		return
	}

	runningConnections = &tmp3

	if s.replicationMode == ReplicationModeSingleSource {
		slaveStatusResult, err := s.rawQuery("SHOW SLAVE STATUS", logger)
		if err != nil {
			s.health.setUP(
				err, ioRunning, wsrepReady, secondsBehindMaster, openConnections, runningConnections, wsrepLocalState,
			)
			return
		}
		rawSecondsBehindMaster := strings.TrimSpace(slaveStatusResult["Seconds_Behind_Master"])
		if rawSecondsBehindMaster == "" || strings.ToLower(rawSecondsBehindMaster) == "null" {
			s.health.setUP(
				fmt.Errorf("empty or null value for Seconds_Behind_Master returned from MySQL: %s", err),
				ioRunning, wsrepReady, secondsBehindMaster, openConnections, runningConnections, wsrepLocalState,
			)
			return
		}

		tmp, err := strconv.Atoi(rawSecondsBehindMaster)
		if err != nil {
			s.health.setUP(
				fmt.Errorf("unexpected value for Seconds_Behind_Master returned from MySQL (conversion error): %s", err),
				ioRunning, wsrepReady, secondsBehindMaster, openConnections, runningConnections, wsrepLocalState,
			)
			return
		}

		secondsBehindMaster = &tmp
	} else if s.replicationMode == ReplicationModeMultiSourceWriteSet {
		writesetStateResult, err := s.rawQuery("SHOW STATUS LIKE 'wsrep_local_state'", logger)
		if err != nil {
			s.health.setUP(
				fmt.Errorf("failed acquiring MySQL wsrep_local_state:  %s", err),
				ioRunning, wsrepReady, secondsBehindMaster, openConnections, runningConnections, wsrepLocalState,
			)
			return
		}

		writesetState := writesetStateResult["Value"]
		tmp, err := strconv.Atoi(writesetState)
		if err != nil {
			s.health.setUP(
				fmt.Errorf("unexpected value for wsrep_local_state returned from MySQL:  %s", err),
				ioRunning, wsrepReady, secondsBehindMaster, openConnections, runningConnections, wsrepLocalState,
			)
			return
		}

		wsrepLocalState = &tmp
	}

	s.health.setUP(nil, ioRunning, wsrepReady, secondsBehindMaster, openConnections, runningConnections, wsrepLocalState)
}

func (s *Server) connectReadUser(traceOn bool, logger Logger) error {
	mutex.Lock()
	defer mutex.Unlock()

	if s.connection == nil {
		conn, err := s.connect(s.serverSettings.DSN, traceOn, logger)
		if err != nil {
			return fmt.Errorf("could not connect to MySQL read user: %s", err.Error())
		}
		s.connection = conn
	}

	return nil
}

func (s *Server) connectReplicationUser(traceOn bool, logger Logger) error {
	mutex.Lock()
	defer mutex.Unlock()

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
