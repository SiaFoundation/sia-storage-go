package siastorage

import (
	"errors"
	"sync"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/indexd/hosts"
)

type hostCache struct {
	mu    sync.Mutex
	hosts map[types.PublicKey]hosts.HostInfo
}

func newHostCache() *hostCache {
	return &hostCache{
		hosts: make(map[types.PublicKey]hosts.HostInfo),
	}
}

// UsableHosts returns all cached hosts.
func (s *hostCache) UsableHosts() ([]hosts.HostInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	hosts := make([]hosts.HostInfo, 0, len(s.hosts))
	for _, host := range s.hosts {
		hosts = append(hosts, host)
	}
	return hosts, nil
}

// Addresses returns the network addresses for the specified host.
func (s *hostCache) Addresses(hk types.PublicKey) ([]chain.NetAddress, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	host, exists := s.hosts[hk]
	if !exists {
		return nil, errors.New("unknown host")
	}
	return host.Addresses, nil
}

// Usable returns whether the host is usable (i.e. cached).
func (s *hostCache) Usable(hk types.PublicKey) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.hosts[hk]
	return exists, nil
}

// updateHosts replaces the cached hosts with the provided update and returns any newly added hosts.
func (s *hostCache) updateHosts(update []hosts.HostInfo) []hosts.HostInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := make(map[types.PublicKey]struct{})
	for hk := range s.hosts {
		existing[hk] = struct{}{}
	}
	s.hosts = make(map[types.PublicKey]hosts.HostInfo)

	var added []hosts.HostInfo
	for _, hi := range update {
		if _, exists := existing[hi.PublicKey]; !exists {
			added = append(added, hi)
		}
		s.hosts[hi.PublicKey] = hi
	}

	return added
}
