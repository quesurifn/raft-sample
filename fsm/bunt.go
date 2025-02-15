package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/hashicorp/raft"
	"github.com/tidwall/buntdb"
)

// buntFSM raft.FSM implementation using BuntDB
type buntFSM struct {
	db *buntdb.DB
}

// get fetch data from BuntDB
func (b buntFSM) get(key string) (interface{}, error) {
	var data interface{}
	err := b.db.View(func(tx *buntdb.Tx) error {
		val, err := tx.Get(key)
		if err != nil {
			return err
		}
		if err := json.Unmarshal([]byte(val), &data); err != nil {
			return nil
		}
		return nil
	})

	if err != nil {
		data = map[string]interface{}{}
		return data, err
	}

	return data, err
}

// set store data to BuntDB
func (b buntFSM) set(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	if data == nil || len(data) <= 0 {
		return nil
	}

	err = b.db.Update(func(tx *buntdb.Tx) error {
		if _, _, err := tx.Set(key, string(data), nil); err != nil {
			return err
		}
		return nil
	})
	return err
}

// delete remove data from BuntDB
func (b buntFSM) delete(key string) error {
	err := b.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(key)
		return err
	})
	return err
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (b buntFSM) Apply(log *raft.Log) interface{} {
	switch log.Type {
	case raft.LogCommand:
		var payload = CommandPayload{}
		if err := json.Unmarshal(log.Data, &payload); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "error marshalling store payload %s\n", err.Error())
			return nil
		}

		op := strings.ToUpper(strings.TrimSpace(payload.Operation))
		switch op {
		case "SET":
			return &ApplyResponse{
				Error: b.set(payload.Key, payload.Value),
				Data:  payload.Value,
			}
		case "GET":
			data, err := b.get(payload.Key)
			return &ApplyResponse{
				Error: err,
				Data:  data,
			}

		case "DELETE":
			return &ApplyResponse{
				Error: b.delete(payload.Key),
				Data:  nil,
			}
		}
	}

	_, _ = fmt.Fprintf(os.Stderr, "not raft log command type\n")
	return nil
}

// Snapshot will be called during make snapshot.
// Snapshot is used to support log compaction.
// No need to call snapshot since it already persisted in disk (using BuntDB) when raft calling Apply function.
func (b buntFSM) Snapshot() (raft.FSMSnapshot, error) {
	return newSnapshotNoop()
}

// Restore is used to restore an FSM from a Snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
// Restore will update all data in BuntDB
func (b buntFSM) Restore(rClose io.ReadCloser) error {
	defer func() {
		if err := rClose.Close(); err != nil {
			_, _ = fmt.Fprintf(os.Stdout, "[FINALLY RESTORE] close error %s\n", err.Error())
		}
	}()

	_, _ = fmt.Fprintf(os.Stdout, "[START RESTORE] read all message from snapshot\n")
	var totalRestored int

	decoder := json.NewDecoder(rClose)
	for decoder.More() {
		var data = &CommandPayload{}
		err := decoder.Decode(data)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stdout, "[END RESTORE] error decode data %s\n", err.Error())
			return err
		}

		if err := b.set(data.Key, data.Value); err != nil {
			_, _ = fmt.Fprintf(os.Stdout, "[END RESTORE] error persist data %s\n", err.Error())
			return err
		}

		totalRestored++
	}

	// read closing bracket
	_, err := decoder.Token()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stdout, "[END RESTORE] error %s\n", err.Error())
		return err
	}

	_, _ = fmt.Fprintf(os.Stdout, "[END RESTORE] success restore %d messages in snapshot\n", totalRestored)
	return nil
}

// NewBuntDB raft.FSM implementation using buntdb
func NewBuntDB(buntDB *buntdb.DB) raft.FSM {
	return &buntFSM{
		db: buntDB,
	}
}
