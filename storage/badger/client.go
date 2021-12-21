package badger

import (
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/sirupsen/logrus"
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

func (c *Client) GC() {
	go func() {
		logrus.Println("Badger ValueLogGC Start.")
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			<-ticker.C
			for {
				if c.db == nil || c.db.IsClosed() {
					logrus.Errorln("Badger ValueLogGC Shutdown. DB is nil or closed.")
					return
				}
				if err := c.db.RunValueLogGC(0.7); err != nil {
					logrus.Debugln(fmt.Sprintf("Badger ValueLogGC Failed. err: %s", err))
					break
				}
			}
			logrus.Debugln("Badger ValueLogGC Completed.")
		}
	}()
}

func (c *Client) Get(key []byte) ([]byte, error) {
	var res []byte
	err := c.db.View(func(txn *badger.Txn) error {
		val, err := txn.Get(key)
		if err != nil {
			return err
		}
		return val.Value(func(v []byte) error {
			res = v
			return nil
		})
	})
	return res, err
}

func (c *Client) Add(key []byte, value []byte) error {
	return c.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (c *Client) Del(key []byte) error {
	return c.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}
