Balancer [![Build Status](https://drone.io/github.com/StudioSol/balancer/status.png)](https://drone.io/github.com/StudioSol/balancer/latest) [![GoDoc](https://godoc.org/github.com/StudioSol/balancer?status.svg)](https://godoc.org/github.com/StudioSol/balancer) [![Go Report Card](https://goreportcard.com/badge/github.com/StudioSol/balancer)](https://goreportcard.com/report/github.com/StudioSol/balancer)
========

MySql Load Balancer


#### Usage

```GO
package main

import (
    "fmt"
    "log"

    "github.com/StudioSol/balancer"
)

func main() {
    config := balancer.Config{
        CheckInterval: 3,
        StartCheck:    true,
        TraceOn:       false,
        Logger:        log,
        ServersSettings:     []balancer.ServerSettings{
            balancer.ServerSettings{
                Name:         "slave 1",
                DSN:          "user:pass@tcp(127.0.0.1:3306)/database",
                MaxIdleConns: 0,
                MaxOpenConns: 10,
            },
            balancer.ServerSettings{
                Name:         "slaves 2",
                DSN:          "user:pass@tcp(127.0.0.1:3306)/database",
                MaxIdleConns: 0,
                MaxOpenConns: 0,
            }
        }
    }

    db := balancer.New(config)

    server := db.PickServer()
    if server != nil {
        fmt.Println("No Server avaliable", server)
    }

    server.GetConnection().SelectOne(...)
}
```
