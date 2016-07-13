package concurrence

import "time"

// Every executes `fn` every `duration`
// To stop the ticker just make `fn` return false
func Every(duration time.Duration, fn func(time.Time) bool) chan bool {
	ticker := time.NewTicker(duration)
	stopFlag := make(chan bool, 1)
	go func() {
		for {
			select {
			case time := <-ticker.C:
				if !fn(time) {
					stopFlag <- true
				}
			case <-stopFlag:
				return
			}
		}
	}()
	return stopFlag
}
