package serverplugin

import (
	"context"
	. "github.com/darrennong/rpcx-consul/client"
	"github.com/smallnest/rpcx/client"
	"log"
	"testing"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/smallnest/rpcx/server"
)

func TestConsulRegistry(t *testing.T) {
	s := server.NewServer()
	r := NewConsulRegisterPlugin(
		WithConsulServiceAddress("tcp@192.168.0.28:8081"),
		WithConsulServers([]string{"127.0.0.1:8500"}),
		WithConsulBasePath("/rpcx_test"),
		WithConsulMetrics(metrics.NewRegistry()),
		WithConsulUpdateInterval(time.Minute),
	)
	s.Plugins.Add(r)

	s.RegisterName("Arith", new(Arith), "")
	defer s.Close()
	s.Serve("tcp", "192.168.0.28:8081")
}

func TestServiceDiscover(t *testing.T) {

	d, _ := NewConsulDiscovery("spiritV2", "LogHouse", []string{"127.0.0.1:8500"}, nil)
	xclient := client.NewXClient("LogHouse", client.Failtry, client.RandomSelect, d, client.DefaultOption)
	defer xclient.Close()

	args := &Args{
		A: 10,
		B: 20,
	}
	reply := &Reply{}
	err := xclient.Call(context.Background(), "StartLog", args, reply)
	if err != nil {
		log.Fatalf("failed to call: %v", err)
	}

	log.Printf("%d * %d = %v", args.A, args.B, reply)
}
