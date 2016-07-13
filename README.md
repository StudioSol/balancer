Balancer
========

MySql Load Balancer


#### Usage

```GOLANG
package main

import (
    "fmt"
    "github.com/StudioSol/Balancer"
)

func main() {
    config := balancer.Config{
        CheckInterval: 3,
        StartCheck:    false,
        TraceOne:      false,
        Addresses:     []balancer.Address{
            balancer.Address{
                Name:       "master",
                ConnString: "user:pass@tcp(127.0.0.1:3306)/database",
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
