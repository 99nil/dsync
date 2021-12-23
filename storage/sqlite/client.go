package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/99nil/dsync"

	_ "github.com/mattn/go-sqlite3"
)

var _ dsync.StorageInterface = (*Client)(nil)

type Config struct {
	Path string `json:"path"`
}

type Client struct {
	db *sql.DB
}

type data struct {
	Key   []byte `gorm:"primary_key;column:uid;type:varchar(40);not null"`
	Space []byte `gorm:"column:space;type:varchar(20);not null"`
	Value []byte `gorm:"column:value;type:text"`
}

var db *sql.DB

const dataTable = "data"

func New(cfg *Config) (*Client, error) {
	var err error
	if db == nil {
		db, err = sql.Open("sqlite3", cfg.Path)
		if err != nil {
			return nil, err
		}
		if _, err = db.Exec(
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (`uid` BLOB(40) PRIMARY KEY, `space` VARCHAR(20) NOT NULL, `value` TEXT)",
				dataTable)); err != nil {
			return nil, err
		}
	}
	return &Client{db: db}, err
}

func (c *Client) Get(ctx context.Context, space, key string) ([]byte, error) {
	var res []byte
	stmt, err := c.db.PrepareContext(ctx, fmt.Sprintf("SELECT `value` FROM %s WHERE `uid`=? AND `space`=?", dataTable))
	defer stmt.Close()
	if err != nil {
		return nil, err
	}
	if err = stmt.QueryRowContext(ctx, key, space).Scan(&res); err != nil {
		return nil, err
	}
	return res, nil
}

func (c *Client) Add(ctx context.Context, space, key string, value []byte) error {
	stmt, err := c.db.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?)", dataTable))
	defer stmt.Close()
	if err != nil {
		return err
	}
	if _, err = stmt.ExecContext(ctx, key, space, value); err != nil {
		return err
	}
	return nil
}

func (c *Client) Del(ctx context.Context, space, key string) error {
	if len(key) == 0 || len(space) == 0 {
		return fmt.Errorf("key or space must not be null")
	}
	stmt, err := c.db.PrepareContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE `uid`=? AND `space`=?", dataTable))
	defer stmt.Close()
	if err != nil {
		return err
	}
	if _, err = stmt.ExecContext(ctx, key, space); err != nil {
		return err
	}
	return nil
}

func (c *Client) List(ctx context.Context, space string) ([]dsync.KV, error) {
	var res []dsync.KV
	stmt, err := c.db.PrepareContext(ctx, fmt.Sprintf("SELECT `uid`,`value` FROM %s WHERE `space`=?", dataTable))
	defer stmt.Close()
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx, space)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var current dsync.KV
		if err = rows.Scan(&current); err != nil {
			return nil, err
		}
		res = append(res, current)
	}
	return res, nil
}
