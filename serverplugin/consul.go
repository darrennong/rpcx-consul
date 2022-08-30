package serverplugin

import (
	"context"
	"fmt"
	consulapi "github.com/hashicorp/consul/api"
	"net"
	"strconv"
	"strings"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/rpcxio/libkv/store"
	"github.com/rpcxio/libkv/store/consul"
	"github.com/smallnest/rpcx/log"
)

func init() {
	consul.Register()
}

// ConsulRegisterPlugin implements consul registry.
type ConsulRegisterPlugin struct {
	// service address, for example, tcp@127.0.0.1:8972, quic@127.0.0.1:1234
	ServiceAddress string
	ServiceHost    string
	ServicePort    int
	// consul addresses
	ConsulServers []string
	// base path for rpcx server, for example com/example/rpcx
	NodeId  string
	Metrics metrics.Registry
	// Registered services
	Services       []string
	Tags           []string
	UpdateInterval time.Duration

	Options *store.Config

	registration *consulapi.AgentServiceRegistration
	//kv      store.Store
	//
	//dying chan struct{}
	//done  chan struct{}
}

type ConsulOpt func(*ConsulRegisterPlugin)

func WithConsulServers(consulServers []string) ConsulOpt {
	return func(o *ConsulRegisterPlugin) {
		o.ConsulServers = consulServers
	}
}

func WithConsulTags(tags ...string) ConsulOpt {
	return func(o *ConsulRegisterPlugin) {
		o.Tags = tags
	}
}

func WithConsulServiceAddress(serviceAddress string) ConsulOpt {
	return func(o *ConsulRegisterPlugin) {
		o.ServiceAddress = serviceAddress
		s := strings.Split(serviceAddress, ":")
		if len(s) == 2 {
			if i, e := strconv.Atoi(s[1]); e == nil {
				o.ServicePort = i
			}
		}
		t := strings.Split(s[0], "@")
		if len(t) == 1 {
			o.ServiceHost = t[0]
		} else {
			o.ServiceHost = t[1]
		}
	}
}

func WithConsulNodeId(basePath string) ConsulOpt {
	return func(o *ConsulRegisterPlugin) {
		o.NodeId = basePath
	}
}

func WithConsulMetrics(me metrics.Registry) ConsulOpt {
	return func(o *ConsulRegisterPlugin) {
		o.Metrics = me
	}
}

func WithConsulUpdateInterval(updateInterval time.Duration) ConsulOpt {
	return func(o *ConsulRegisterPlugin) {
		o.UpdateInterval = updateInterval
	}
}

func WithConsulOptions(options *store.Config) ConsulOpt {
	return func(o *ConsulRegisterPlugin) {
		o.Options = options
	}
}

func NewConsulRegisterPlugin(o ...ConsulOpt) *ConsulRegisterPlugin {
	consulPlugin := &ConsulRegisterPlugin{}
	consulPlugin.registration = new(consulapi.AgentServiceRegistration)
	for _, v := range o {
		v(consulPlugin)
	}
	return consulPlugin
}

// Stop unregister all services.
func (p *ConsulRegisterPlugin) Stop() error {
	// 创建连接consul服务配置
	config := consulapi.DefaultConfig()
	config.Address = p.ConsulServers[0]
	client, err := consulapi.NewClient(config)
	if err != nil {
		log.Fatal("consul client error : ", err)
	}
	client.Agent().ServiceDeregister("server1")
	return err
}

// HandleConnAccept handles connections from clients
func (p *ConsulRegisterPlugin) HandleConnAccept(conn net.Conn) (net.Conn, bool) {
	if p.Metrics != nil {
		metrics.GetOrRegisterMeter("connections", p.Metrics).Mark(1)
	}
	return conn, true
}

// PreCall handles rpc call from clients
func (p *ConsulRegisterPlugin) PreCall(_ context.Context, _, _ string, args interface{}) (interface{}, error) {
	if p.Metrics != nil {
		metrics.GetOrRegisterMeter("calls", p.Metrics).Mark(1)
	}
	return args, nil
}

// Register handles registering event.
// this service is registered at BASE/serviceName/thisIpAddress node
func (p *ConsulRegisterPlugin) Register(name string, rcvr interface{}, metadata string) (err error) {
	/*
		if strings.TrimSpace(name) == "" {
			err = errors.New("Register service `name` can't be empty")
			return
		}

		if p.kv == nil {
			consul.Register()
			kv, err := libkv.NewStore(store.CONSUL, p.ConsulServers, nil)
			if err != nil {
				log.Errorf("cannot create consul registry: %v", err)
				return err
			}
			p.kv = kv
		}

		if p.BasePath[0] == '/' {
			p.BasePath = p.BasePath[1:]
		}
		err = p.kv.Put(p.BasePath, []byte("rpcx_path"), &store.WriteOptions{IsDir: true})
		if err != nil {
			log.Errorf("cannot create consul path %s: %v", p.BasePath, err)
			return err
		}

		nodePath := fmt.Sprintf("%s/%s", p.BasePath, name)
		err = p.kv.Put(nodePath, []byte(name), &store.WriteOptions{IsDir: true})
		if err != nil {
			log.Errorf("cannot create consul path %s: %v", nodePath, err)
			return err
		}

		nodePath = fmt.Sprintf("%s/%s/%s", p.BasePath, name, p.ServiceAddress)
		err = p.kv.Put(nodePath, []byte(metadata), &store.WriteOptions{TTL: p.UpdateInterval * 2})
		if err != nil {
			log.Errorf("cannot create consul path %s: %v", nodePath, err)
			return err
		}

		p.Services = append(p.Services, name)

		p.metasLock.Lock()
		if p.metas == nil {
			p.metas = make(map[string]string)
		}
		p.metas[name] = metadata
		p.metasLock.Unlock()
		return
	*/
	// 创建连接consul服务配置
	config := consulapi.DefaultConfig()
	config.Address = p.ConsulServers[0]
	client, err := consulapi.NewClient(config)
	if err != nil {
		log.Fatal("consul client error : ", err)
	}
	// 创建注册到consul的服务到
	p.registration.ID = p.NodeId           // 服务节点的名称
	p.registration.Name = name             // 服务名称
	p.registration.Port = p.ServicePort    // 服务端口
	p.registration.Tags = p.Tags           // tag，可以为空
	p.registration.Address = p.ServiceHost // 服务 IP 要确保consul可以访问这个ip
	p.registration.SocketPath = p.ServiceAddress

	// 增加consul健康检查回调函数
	check := new(consulapi.AgentServiceCheck)
	check.CheckID = "api"
	check.Name = p.ServiceAddress
	check.HTTP = fmt.Sprintf("http://%s:%d/health", p.registration.Address, p.registration.Port)
	check.Timeout = "5s"
	check.Interval = "5s"                         // 健康检查间隔
	check.DeregisterCriticalServiceAfter = "300s" // 故障检查失败30s后 consul自动将注册服务删除
	p.registration.Check = check

	// 注册服务到consul
	err = client.Agent().ServiceRegister(p.registration)
	if err == nil {
		fmt.Println("ConsulRegister done")
	}
	return err
}

func (p *ConsulRegisterPlugin) RegisterFunction(serviceName, fname string, fn interface{}, metadata string) error {
	return p.Register(serviceName, fn, metadata)
}

func (p *ConsulRegisterPlugin) Unregister(name string) (err error) {
	//if len(p.Services) == 0 {
	//	return nil
	//}
	//if strings.TrimSpace(name) == "" {
	//	err = errors.New("Unregister service `name` can't be empty")
	//	return
	//}
	//
	//if p.kv == nil {
	//	consul.Register()
	//	kv, err := libkv.NewStore(store.CONSUL, p.ConsulServers, nil)
	//	if err != nil {
	//		log.Errorf("cannot create consul registry: %v", err)
	//		return err
	//	}
	//	p.kv = kv
	//}
	//
	//if p.BasePath[0] == '/' {
	//	p.BasePath = p.BasePath[1:]
	//}
	//err = p.kv.Put(p.BasePath, []byte("rpcx_path"), &store.WriteOptions{IsDir: true})
	//if err != nil {
	//	log.Errorf("cannot create consul path %s: %v", p.BasePath, err)
	//	return err
	//}
	//
	//nodePath := fmt.Sprintf("%s/%s", p.BasePath, name)
	//
	//err = p.kv.Put(nodePath, []byte(name), &store.WriteOptions{IsDir: true})
	//if err != nil {
	//	log.Errorf("cannot create consul path %s: %v", nodePath, err)
	//	return err
	//}
	//
	//nodePath = fmt.Sprintf("%s/%s/%s", p.BasePath, name, p.ServiceAddress)
	//
	//err = p.kv.Delete(nodePath)
	//if err != nil {
	//	log.Errorf("cannot remove consul path %s: %v", nodePath, err)
	//	return err
	//}
	//
	//var services = make([]string, 0, len(p.Services)-1)
	//for _, s := range p.Services {
	//	if s != name {
	//		services = append(services, s)
	//	}
	//}
	//p.Services = services
	//
	//p.metasLock.Lock()
	//if p.metas == nil {
	//	p.metas = make(map[string]string)
	//}
	//delete(p.metas, name)
	//p.metasLock.Unlock()
	return
}
