package balancer

import (
	"database/sql"
	"strconv"

	"github.com/go-gorp/gorp"
)

type Server struct {
	name       string
	address    Address
	health     *ServerHealth
	connection *gorp.DbMap
	config     *Config
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

func (s *Server) connectIfNecessary() error {
	if s.connection == nil {
		conn, err := sql.Open("mysql", s.address.ConnString)
		if err != nil {
			return err
		}

		conn.SetMaxIdleConns(s.address.MaxIdleConns)
		conn.SetMaxOpenConns(s.address.MaxOpenConns)

		if err := conn.Ping(); err != nil {
			return err
		}

		s.connection = &gorp.DbMap{
			Db:      conn,
			Dialect: gorp.MySQLDialect{},
		}

		if s.config.TraceOn {
			s.connection.TraceOn("[sql]", s.config.Logger)
		}
	}
	return nil
}

func (s *Server) rawQuery(query string) (map[string]string, error) {
	rows, err := s.connection.Db.Query(query)
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, sql.ErrNoRows
	}
	defer func() {
		if err := rows.Close(); err != nil {
			s.config.Logger.Printf(err.Error())
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

func (s *Server) CheckHealth() {
	if err := s.connectIfNecessary(); err != nil {
		s.health.setDown(err)
		return
	}

	if slaveStatusResult, err := s.rawQuery("SHOW SLAVE STATUS"); err == nil {
		secondsBehindMaster := slaveStatusResult["Seconds_Behind_Master"]
		if secondsBehindMaster != "" && secondsBehindMaster != "NULL" {
			if v, err := strconv.Atoi(secondsBehindMaster); err == nil {
				s.health.mutex.Lock()
				s.health.secondsBehindMaster = &v
				s.health.mutex.Unlock()
			}
		}
	}

	threadsConnectedResult, err := s.rawQuery("SHOW STATUS LIKE 'Threads_connected'")
	if err != nil {
		s.health.setDown(err)
		return
	}

	threadsConnected := threadsConnectedResult["Value"]
	v, err := strconv.Atoi(threadsConnected)
	if err != nil {
		s.health.setDown(err)
		return
	}

	s.health.mutex.Lock()
	s.health.openConnections = v
	s.health.mutex.Unlock()
	s.health.setUP()
}
