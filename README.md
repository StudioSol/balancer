## MySQL Load Balancer 

[![Build Status](https://travis-ci.org/StudioSol/balancer.svg?branch=master)](https://travis-ci.org/StudioSol/balancer)
[![codecov](https://codecov.io/gh/StudioSol/balancer/branch/master/graph/badge.svg)](https://codecov.io/gh/StudioSol/balancer)
[![GoDoc](https://godoc.org/github.com/StudioSol/balancer?status.svg)](https://godoc.org/github.com/StudioSol/balancer)
[![Go Report Card](https://goreportcard.com/badge/github.com/StudioSol/balancer)](https://goreportcard.com/report/github.com/StudioSol/balancer)

### Usage

```go
package main

import (
    "fmt"
    "log"

    "github.com/StudioSol/balancer"
)

func main() {
    config := balancer.Config{
        // Time in seconds in wich the health of the slaves is going to be checked
        CheckInterval: 3,

        // Wether the balancer should start checking health
        StartCheck: true,

        // Wether the queries executed by the balancer server should be logged
        TraceOn: false,

        // A balancer.Logger interface implementation
        Logger: log,

		// Slave servers' configuration
        ServersSettings: []balancer.ServerSettings{
            balancer.ServerSettings{
			// Name of the MySQL Slave Server
			Name: "slave 1",

			// Connection string of the MySQL user used for reading
			DSN: "user:password@tcp(127.0.0.1:3306)/database",

			// Connection string of the MySQL user used for status. The chosen
			// user must have "REPLICATION STATUS" permission
			ReplicationDSN: "replication_user:password@tcp(127.0.0.1:3306)/",

			// Maximum idle connections
			MaxIdleConns: 0,

			// Maximum open connections
			MaxOpenConns: 10,
		},
		// ...
    }

    db := balancer.New(config)

    server := db.PickServer()
    if server != nil {
        fmt.Println("No Server avaliable", server)
    }

    // Be happy! :)
    server.GetConnection().SelectOne(
    	// ...
    )
}
```
