package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"
	"ysf/raftsample/fsm"
	"ysf/raftsample/server"

	"github.com/hashicorp/raft"
	"github.com/spf13/viper"
	"github.com/tidwall/buntdb"
	fastlog "github.com/tidwall/raft-fastlog"
)

// configRaft configuration for raft node
type configRaft struct {
	NodeId    string `mapstructure:"node_id"`
	Port      int    `mapstructure:"port"`
	VolumeDir string `mapstructure:"volume_dir"`
}

// configServer configuration for HTTP server
type configServer struct {
	Port int `mapstructure:"port"`
}

// config configuration
type config struct {
	Server configServer `mapstructure:"server"`
	Raft   configRaft   `mapstructure:"raft"`
}

const (
	serverPort = "SERVER_PORT"

	raftNodeId = "RAFT_NODE_ID"
	raftPort   = "RAFT_PORT"
	raftVolDir = "RAFT_VOL_DIR"
)

var confKeys = []string{
	serverPort,

	raftNodeId,
	raftPort,
	raftVolDir,
}

const (
	// The maxPool controls how many connections we will pool.
	maxPool = 3

	// The timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	// https://github.com/hashicorp/raft/blob/v1.1.2/net_transport.go#L177-L181
	tcpTimeout = 10 * time.Second

	// The `retain` parameter controls how many
	// snapshots are retained. Must be at least 1.
	raftSnapShotRetain = 2

	// raftLogCacheSize is the maximum number of logs to cache in-memory.
	// This is used to reduce disk I/O for the recently committed entries.
	raftLogCacheSize = 512
)

// main entry point of application start
// run using CONFIG=config.yaml ./program
func main() {

	var v = viper.New()
	v.AutomaticEnv()
	if err := v.BindEnv(confKeys...); err != nil {
		log.Fatal(err)
		return
	}

	conf := config{
		Server: configServer{
			Port: v.GetInt(serverPort),
		},
		Raft: configRaft{
			NodeId:    v.GetString(raftNodeId),
			Port:      v.GetInt(raftPort),
			VolumeDir: v.GetString(raftVolDir),
		},
	}

	log.Printf("%+v\n", conf)

	// Preparing buntdb
	buntDB, err := buntdb.Open("data.db")
	if err != nil {
		log.Fatal(err)
		return
	}

	defer func() {
		if err := buntDB.Close(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "error close buntDB: %s\n", err.Error())
		}
	}()

	var raftBinAddr = fmt.Sprintf("127.0.0.1:%d", conf.Raft.Port)

	raftConf := raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(conf.Raft.NodeId)
	raftConf.SnapshotThreshold = 1024

	fsmStore := fsm.NewBuntDB(buntDB)

	if _, err := os.Stat(conf.Raft.VolumeDir); os.IsNotExist(err) {
		os.Mkdir(conf.Raft.VolumeDir, 0755)
	}

	store, err := fastlog.NewFastLogStore(filepath.Join(conf.Raft.VolumeDir, "raft.dataRepo"), fastlog.High, nil)
	if err != nil {
		log.Fatal(err)
		return
	}

	// Wrap the store in a LogCache to improve performance.
	cacheStore, err := raft.NewLogCache(raftLogCacheSize, store)
	if err != nil {
		log.Fatal(err)
		return
	}

	snapshotStore, err := raft.NewFileSnapshotStore(conf.Raft.VolumeDir, raftSnapShotRetain, os.Stdout)
	if err != nil {
		log.Fatal(err)
		return
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", raftBinAddr)
	if err != nil {
		log.Fatal(err)
		return
	}
	transport, err := raft.NewTCPTransport(raftBinAddr, tcpAddr, maxPool, tcpTimeout, os.Stdout)
	if err != nil {
		log.Fatal(err)
		return
	}

	raftServer, err := raft.NewRaft(raftConf, fsmStore, cacheStore, store, snapshotStore, transport)
	if err != nil {
		log.Fatal(err)
		return
	}

	// always start single server as a leader
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(conf.Raft.NodeId),
				Address: transport.LocalAddr(),
			},
		},
	}

	raftServer.BootstrapCluster(configuration)

	srv := server.New(fmt.Sprintf(":%d", conf.Server.Port), buntDB, raftServer)
	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}

	return
}
