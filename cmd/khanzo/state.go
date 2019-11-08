package main

import (
	"sync"

	"github.com/rekki/blackrock/cmd/orgrim/spec"
)

type State struct {
	data map[string]map[string]*spec.Context
	sync.RWMutex
}

func NewState() *State {
	return &State{data: map[string]map[string]*spec.Context{}}
}

func (s *State) SetMany(data []*spec.Context) {
	s.Lock()
	defer s.Unlock()
	for _, c := range data {
		x, ok := s.data[c.ForeignType]
		if !ok {
			x = map[string]*spec.Context{}
			s.data[c.ForeignType] = x
		}
		x[c.ForeignId] = c

	}
}

func (s *State) Get(foreignType, foreignId string) *spec.Context {
	s.RLock()
	defer s.RUnlock()
	x, ok := s.data[foreignType]
	if !ok {
		return nil
	}
	return x[foreignId]
}
