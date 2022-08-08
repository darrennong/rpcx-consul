package client

import (
	"fmt"
	consulapi "github.com/hashicorp/consul/api"
	"strings"
	"sync"
	"time"

	"github.com/rpcxio/libkv/store"
	"github.com/rpcxio/libkv/store/consul"
	"github.com/smallnest/rpcx/client"
	"github.com/smallnest/rpcx/log"
)

func init() {
	consul.Register()
}

// ConsulDiscovery is a consul service discovery.
// It always returns the registered servers in consul.
type ConsulDiscovery struct {
	basePath string
	kv       store.Store
	pairsMu  sync.RWMutex
	pairs    []*client.KVPair
	chans    []chan []*client.KVPair
	mu       sync.Mutex
	// -1 means it always retry to watch until zookeeper is ok, 0 means no retry.
	RetriesAfterWatchFailed int

	filter client.ServiceDiscoveryFilter

	stopCh chan struct{}
}

// NewConsulDiscovery returns a new ConsulDiscovery.
func NewConsulDiscovery(basePath, servicePath string, consulAddr []string, options *store.Config) (*ConsulDiscovery, error) {
	// 创建连接consul服务配置
	config := consulapi.DefaultConfig()
	config.Address = consulAddr[0]
	cc, err := consulapi.NewClient(config)
	if err != nil {
		log.Fatal("consul client error : ", err)
	}
	// 获取所有service
	services, _ := cc.Agent().Services()
	pairs := make([]*client.KVPair, 0, 0)
	for _, value := range services {
		fmt.Println("address:", value.Address)
		fmt.Println("port:", value.Port)
		if value.Service == servicePath {
			kvp := client.KVPair{Key: value.SocketPath, Value: value.ID}
			pairs = append(
				pairs,
				&kvp,
			)
		}
	}

	fmt.Println("=================================")
	d := &ConsulDiscovery{}
	d.pairsMu.Lock()
	d.pairs = pairs
	d.pairsMu.Unlock()
	return d, err
}

// Clone clones this ServiceDiscovery with new servicePath.
func (d *ConsulDiscovery) Clone(servicePath string) (client.ServiceDiscovery, error) {
	cd := &ConsulDiscovery{pairs: d.pairs}
	return cd, nil
}

// SetFilter sets the filer.
func (d *ConsulDiscovery) SetFilter(filter client.ServiceDiscoveryFilter) {
	d.filter = filter
}

// GetServices returns the servers
func (d *ConsulDiscovery) GetServices() []*client.KVPair {
	d.pairsMu.RLock()
	defer d.pairsMu.RUnlock()
	return d.pairs
}

// WatchService returns a nil chan.
func (d *ConsulDiscovery) WatchService() chan []*client.KVPair {
	d.mu.Lock()
	defer d.mu.Unlock()

	ch := make(chan []*client.KVPair, 10)
	d.chans = append(d.chans, ch)
	return ch
}

func (d *ConsulDiscovery) RemoveWatcher(ch chan []*client.KVPair) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var chans []chan []*client.KVPair
	for _, c := range d.chans {
		if c == ch {
			continue
		}

		chans = append(chans, c)
	}

	d.chans = chans
}

func (d *ConsulDiscovery) watch() {
	defer func() {
		d.kv.Close()
	}()
	for {
		var err error
		var c <-chan []*store.KVPair
		var tempDelay time.Duration

		retry := d.RetriesAfterWatchFailed
		for d.RetriesAfterWatchFailed < 0 || retry >= 0 {
			c, err = d.kv.WatchTree(d.basePath, d.stopCh)
			if err != nil {
				if d.RetriesAfterWatchFailed > 0 {
					retry--
				}
				if tempDelay == 0 {
					tempDelay = 1 * time.Second
				} else {
					tempDelay *= 2
				}
				if max := 30 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Warnf("can not watchtree (with retry %d, sleep %v): %s: %v", retry, tempDelay, d.basePath, err)
				time.Sleep(tempDelay)
				continue
			}
			break
		}

		if err != nil {
			log.Errorf("can't watch %s: %v", d.basePath, err)
			return
		}

		prefix := d.basePath + "/"

	readChanges:
		for {
			select {
			case <-d.stopCh:
				log.Info("discovery has been closed")
				return
			case ps, ok := <-c:
				if !ok {
					break readChanges
				}
				var pairs []*client.KVPair // latest servers
				if ps == nil {
					d.pairsMu.Lock()
					d.pairs = pairs
					d.pairsMu.Unlock()
					continue
				}
				for _, p := range ps {
					if !strings.HasPrefix(p.Key, prefix) { // avoid prefix issue of consul List
						continue
					}
					k := strings.TrimPrefix(p.Key, prefix)
					pair := &client.KVPair{Key: k, Value: string(p.Value)}
					if d.filter != nil && !d.filter(pair) {
						continue
					}
					pairs = append(pairs, pair)
				}
				d.pairsMu.Lock()
				d.pairs = pairs
				d.pairsMu.Unlock()

				d.mu.Lock()
				for _, ch := range d.chans {
					ch := ch
					go func() {
						defer func() {
							recover()
						}()
						select {
						case ch <- pairs:
						case <-time.After(time.Minute):
							log.Warn("chan is full and new change has been dropped")
						}
					}()
				}
				d.mu.Unlock()
			}
		}

		log.Warn("chan is closed and will rewatch")
	}
}

func (d *ConsulDiscovery) Close() {
	close(d.stopCh)
}
