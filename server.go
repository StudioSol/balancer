package balancer

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-gorp/gorp"
)

type Server struct {
	name           string
	health         *ServerHealth
	serverSettings ServerSettings
	connection     *gorp.DbMap
	traceOn        bool
}

func (s *Server) GetName() string {
	return s.name
}

func (s *Server) GetHealth() *ServerHealth {
	return s.health
}

func (s *Server) GetConnection() *gorp.DbMap {
	return s.connection
}

func (s *Server) connectIfNecessary(traceOn bool, logger Logger) error {
	if s.connection == nil {
		conn, err := sql.Open("mysql", s.serverSettings.DSN)
		if err != nil {
			return err
		}

		conn.SetMaxIdleConns(s.serverSettings.MaxIdleConns)
		conn.SetMaxOpenConns(s.serverSettings.MaxOpenConns)

		if err := conn.Ping(); err != nil {
			return err
		}

		s.connection = &gorp.DbMap{
			Db:      conn,
			Dialect: gorp.MySQLDialect{},
		}

		if traceOn && logger != nil {
			s.connection.TraceOn("[sql]", logger)
		}
	}
	return nil
}

func (s *Server) rawQuery(query string, logger Logger) (map[string]string, error) {
	rows, err := s.connection.Db.Query(query)
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

func (s *Server) CheckHealth(traceOn bool, logger Logger) {
	var secondsBehindMaster, openConnections *int

	if err := s.connectIfNecessary(traceOn, logger); err != nil {
		s.health.setDown(
			fmt.Errorf("error acquiring MySQL connection: %s", err),
			secondsBehindMaster, openConnections,
		)
		return
	}

	slaveStatusResult, err := s.rawQuery("SHOW SLAVE STATUS", logger)
	if err == nil {
		rawSecondsBehindMaster := strings.TrimSpace(slaveStatusResult["Seconds_Behind_Master"])
		if rawSecondsBehindMaster == "" || strings.ToLower(rawSecondsBehindMaster) == "null" {
			s.health.setDown(
				fmt.Errorf("empty or null value for Seconds_Behind_Master returned from MySQL: %s", err),
				secondsBehindMaster, openConnections,
			)
			return
		}

		tmp, err := strconv.Atoi(rawSecondsBehindMaster)
		if err != nil {
			s.health.setDown(
				fmt.Errorf("unexpected value for Seconds_Behind_Master returned from MySQL (conversion error): %s", err),
				secondsBehindMaster, openConnections,
			)
			return
		}

		secondsBehindMaster = &tmp
	}

	threadsConnectedResult, err := s.rawQuery("SHOW STATUS LIKE 'Threads_connected'", logger)
	if err != nil {
		s.health.setDown(
			fmt.Errorf("failed acquiring MySQL thread status:  %s", err),
			secondsBehindMaster, openConnections,
		)
		return
	}

	threadsConnected := threadsConnectedResult["Value"]
	tmp2, err := strconv.Atoi(threadsConnected)
	if err != nil {
		s.health.setDown(
			fmt.Errorf("unexpected value for Threads_connected returned from MySQL:  %s", err),
			secondsBehindMaster, openConnections,
		)
		return
	}
	
	openConnections = &tmp2

	s.health.setUP(secondsBehindMaster, openConnections)
}
