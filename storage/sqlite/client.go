package sqlite

import (
	"database/sql"
	"fmt"

	"github.com/99nil/dsync"

	_ "github.com/mattn/go-sqlite3"
)

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
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (`uid` VARCHAR(40) PRIMARY KEY, `space` VARCHAR(20) NOT NULL, `value` TEXT)",
				dataTable)); err != nil {
			return nil, err
		}
	}
	return &Client{db: db}, err
}

func (c *Client) Get(space, key []byte) ([]byte, error) {
	var res []byte
	stmt, err := c.db.Prepare(fmt.Sprintf("SELECT `value` FROM %s WHERE `uid`=? AND `space`=?", dataTable))
	defer stmt.Close()
	if err != nil {
		return nil, err
	}
	if err = stmt.QueryRow(key, space).Scan(&res); err != nil {
		return nil, err
	}
	return res, nil
}

func (c *Client) Add(space, key, value []byte) error {
	stmt, err := c.db.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?)", dataTable))
	defer stmt.Close()
	if err != nil {
		return err
	}
	if _, err = stmt.Exec(key, space, value); err != nil {
		return err
	}
	return nil
}

func (c *Client) Del(space, key []byte) error {
	if len(key) == 0 || len(space) == 0 {
		return fmt.Errorf("key or space must not be null")
	}
	stmt, err := c.db.Prepare(fmt.Sprintf("DELETE FROM %s WHERE `uid`=? AND `space`=?", dataTable))
	defer stmt.Close()
	if err != nil {
		return err
	}
	if _, err = stmt.Exec(key, space); err != nil {
		return err
	}
	return nil
}

func (c *Client) List(space []byte) ([]dsync.KV, error) {
	var res []dsync.KV
	stmt, err := c.db.Prepare(fmt.Sprintf("SELECT `uid`,`value` FROM %s WHERE `space`=?", dataTable))
	defer stmt.Close()
	if err != nil {
		return nil, err
	}
	rows, err := stmt.Query(space)
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
