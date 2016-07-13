Balancer [![Build Status](https://drone.io/github.com/StudioSol/balancer/status.png)](https://drone.io/github.com/StudioSol/balancer/latest)
========

MySql Load Balancer


#### Usage

```GO
package main

import (
    "fmt"
    "github.com/StudioSol/balancer"
)

func main() {
    config := balancer.Config{
        CheckInterval: 3,
        StartCheck:    false,
        TraceOn:       false,
        Logger:        nil,
        Addresses:     []balancer.Address{
            balancer.Address{
                Name:         "master",
                ConnString:   "user:pass@tcp(127.0.0.1:3306)/database",
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
