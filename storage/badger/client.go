package badger

import (
	"context"
	"time"

	"github.com/dgraph-io/badger/v3"
)

type Config struct {
	Path string `json:"path"`
}

type Client struct {
	db *badger.DB
}

func New(cfg *Config) (*Client, error) {
	options := badger.DefaultOptions(cfg.Path)
	options.Logger = nil
	options.BypassLockGuard = true
	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}
	client := &Client{db: db}
	client.GC()
	return client, nil
}

func (c *Client) Close() error {
	return c.db.Close()
}

func (c *Client) GC() {
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			<-ticker.C
			for {
				if c.db == nil || c.db.IsClosed() {
					return
				}
				if err := c.db.RunValueLogGC(0.7); err != nil {
					break
				}
			}
		}
	}()
}

func (c *Client) Get(_ context.Context, space, key string) ([]byte, error) {
	var res []byte
	err := c.db.View(func(txn *badger.Txn) error {
		prefix := buildPrefix(space)
		fmtKey := append(prefix, key...)
		item, err := txn.Get(fmtKey)
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			res = v
			return nil
		})
	})
	return res, err
}

func (c *Client) Add(_ context.Context, space, key string, value []byte) error {
	return c.db.Update(func(txn *badger.Txn) error {
		fmtKey := append(buildPrefix(space), key...)
		return txn.Set(fmtKey, value)
	})
}

func (c *Client) Del(_ context.Context, space, key string) error {
	return c.db.Update(func(txn *badger.Txn) error {
		fmtKey := append(buildPrefix(space), key...)
		return txn.Delete(fmtKey)
	})
}

func buildPrefix(space string) []byte {
	return append([]byte(space), '-')
}
