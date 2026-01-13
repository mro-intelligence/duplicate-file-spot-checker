package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type scanStats struct {
	stats map[string]int64
	mut   sync.Mutex
}

func newScanStats() *scanStats {
	return &scanStats{
		stats: make(map[string]int64),
		mut:   sync.Mutex{},
	}
}

func (s *scanStats) incrementStat(name string) {
	// TOOD: use sync.Map
	s.mut.Lock()
	defer s.mut.Unlock()
	_, in := s.stats[name]
	if !in {
		s.stats[name] = 0
	}
	s.stats[name]++
}

func (s *scanStats) dump() string {
	s.mut.Lock()
	defer s.mut.Unlock()
	if len(s.stats) == 0 {
		return "No files stats"
	}

	// Sort keys for consistent output
	keys := make([]string, 0, len(s.stats))
	for k := range s.stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var result strings.Builder
	result.WriteString("Skipped files:\n")
	for _, k := range keys {
		result.WriteString(fmt.Sprintf("  %s: %d\n", k, s.stats[k]))
	}
	return result.String()
}
