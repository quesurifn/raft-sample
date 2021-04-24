package store_handler

import (
	"github.com/hashicorp/raft"
	"github.com/tidwall/buntdb"
)

// handler struct handler
type handler struct {
	raft *raft.Raft
	db   *buntdb.DB
}

func New(raft *raft.Raft, db *buntdb.DB) *handler {
	return &handler{
		raft: raft,
		db:   db,
	}
}
