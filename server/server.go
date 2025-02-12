package server

import (
	"net/http"
	_ "net/http/pprof"
	"time"
	"ysf/raftsample/server/raft_handler"
	"ysf/raftsample/server/store_handler"

	"github.com/tidwall/buntdb"

	"github.com/hashicorp/raft"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// srv struct handling server
type srv struct {
	listenAddress string
	raft          *raft.Raft
	echo          *echo.Echo
}

// Start start the server
func (s srv) Start() error {
	return s.echo.StartServer(&http.Server{
		Addr:         s.listenAddress,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
}

// New return new server
func New(listenAddr string, buntDB *buntdb.DB, r *raft.Raft) *srv {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Pre(middleware.RemoveTrailingSlash())
	e.GET("/debug/pprof/*", echo.WrapHandler(http.DefaultServeMux))

	// Raft server
	raftHandler := raft_handler.New(r)
	e.POST("/raft/join", raftHandler.JoinRaftHandler)
	e.POST("/raft/remove", raftHandler.RemoveRaftHandler)
	e.GET("/raft/stats", raftHandler.StatsRaftHandler)

	// Store server
	storeHandler := store_handler.New(r, buntDB)
	e.POST("/store", storeHandler.Store)
	e.GET("/store/:key", storeHandler.Get)
	e.DELETE("/store/:key", storeHandler.Delete)

	return &srv{
		listenAddress: listenAddr,
		echo:          e,
		raft:          r,
	}
}
