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

// updateHosts replaces the cached hosts with the provided update and
// returns hosts that should be warmed up, those hosts are new and good
// for upload, or became good for upload.
func (s *hostCache) updateHosts(update []hosts.HostInfo) []hosts.HostInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	// collect existing hosts
	existing := make(map[types.PublicKey]bool)
	for hk, hi := range s.hosts {
		existing[hk] = hi.GoodForUpload
	}

	// clear hosts
	s.hosts = make(map[types.PublicKey]hosts.HostInfo)

	// add new hosts and track which
	var warmup []hosts.HostInfo
	for _, hi := range update {
		gfu, exists := existing[hi.PublicKey]
		turnedGFU := exists && !gfu && hi.GoodForUpload
		addedGFU := !exists && hi.GoodForUpload
		if turnedGFU || addedGFU {
			warmup = append(warmup, hi)
		}

		s.hosts[hi.PublicKey] = hi
	}

	return warmup
}
