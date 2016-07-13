package balancer

import (
	"math"
	"sort"
)

// Servers - list of servers
type Servers []*Server

func (s Servers) eachASYNC(fn func(int, *Server)) Servers {
	for i, n := range s {
		go fn(i, n)
	}
	return s
}

func (s Servers) filterBySecondsBehindMaster() Servers {
	minValue := math.MaxInt64
	indexesByValue := make(map[int][]int)

	for i := 0; i < len(s); i++ {
		current := s[i].health.secondsBehindMaster
		if current == nil {
			continue
		}

		indexesByValue[*current] = append(indexesByValue[*current], i)
		if *current < minValue {
			minValue = *current
		}
	}

	var filteredServers Servers
	for i := range s {
		for _, index := range indexesByValue[minValue] {
			if i != index {
				continue
			}
			filteredServers = append(filteredServers, s[i])
		}
	}

	sort.Sort(bySecondsBehindMaster(filteredServers))
	return filteredServers
}
