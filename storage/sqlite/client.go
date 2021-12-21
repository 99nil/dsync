package sqlite

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

type Config struct {
	Path string `json:"path"`
}

type Client struct {
	db *sql.DB
}

type data struct {
	Key   []byte `gorm:"primary_key;column;uid;type:varchar(20);not null"`
	Value []byte `gorm:"column:value;type:text"`
}

var db *sql.DB

const dataTable = "data"

func New(cfg *Config) (*Client, error) {
	var err error
	if db == nil {
		db, err = sql.Open("sqlite3", cfg.Path)
		if _, err := db.Exec(
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (`uid` VARCHAR(20) PRIMARY KEY, `value` TEXT)",
				dataTable)); err != nil {
			return nil, err
		}
	}
	return &Client{db: db}, err
}

func (c *Client) Get(key []byte) ([]byte, error) {
	var res []byte
	stmt, err := c.db.Prepare(fmt.Sprintf("SELECT `value` FROM %s WHERE `uid`=?", dataTable))
	defer stmt.Close()
	if err != nil {
		return nil, err
	}
	if err := stmt.QueryRow(key).Scan(&res); err != nil {
		return nil, err
	}
	return res, nil
}

func (c *Client) Add(key []byte, value []byte) error {
	stmt, err := c.db.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", dataTable))
	defer stmt.Close()
	if err != nil {
		return err
	}
	if _, err = stmt.Exec(key, value); err != nil {
		return err
	}
	return nil
}

func (c *Client) Del(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key must not be null")
	}
	stmt, err := c.db.Prepare(fmt.Sprintf("DELETE FROM %s WHERE `uid`=?", dataTable))
	defer stmt.Close()
	if err != nil {
		return err
	}
	if _, err = stmt.Exec(key); err != nil {
		return err
	}
	return nil
}
