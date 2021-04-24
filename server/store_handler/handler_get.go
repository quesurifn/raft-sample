package store_handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/tidwall/buntdb"

	"github.com/labstack/echo/v4"
)

// Get will fetched data from BuntDB where the raft use to store data.
// It can be done in any raft server, making the Get returned eventual consistency on read.
func (h handler) Get(eCtx echo.Context) error {
	var data interface{}
	var key = strings.TrimSpace(eCtx.Param("key"))
	if key == "" {
		return eCtx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"error": "key is empty",
		})
	}

	err := h.db.View(func(tx *buntdb.Tx) error {
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
		return eCtx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"error": fmt.Sprintf("error getting key %s from storage: %s", key, err.Error()),
		})
	}

	return eCtx.JSON(http.StatusOK, map[string]interface{}{
		"message": "success fetching data",
		"data": map[string]interface{}{
			"key":   key,
			"value": data,
		},
	})
}
