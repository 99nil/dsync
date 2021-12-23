// Copyright © 2021 zc2638 <zc2638@qq.com>.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dsync

const (
	keyPrefix = "dsync_"
	keyState  = keyPrefix + "state"
)

var (
	defaultSpace    = []byte("dsync")
	syncerSpace     = []byte(keyPrefix + "syncer")
	dataSetTmpSpace = []byte(keyPrefix + "dataset_tmp")
	dataSetSpace    = []byte(keyPrefix + "dataset")
)

type KV struct {
	Key   []byte
	Value []byte
}

// StorageInterface defines storage related interfaces
type StorageInterface interface {
	// List lists all data in current space
	List(space []byte) ([]KV, error)

	// Get gets data according to the specified key in current space
	Get(space, key []byte) ([]byte, error)

	// Add adds a set of key/value pairs in current space
	Add(space, key, value []byte) error

	// Del deletes key/value pairs according to the specified key in current space
	Del(space, key []byte) error
}

// Interface defines dsync core
type Interface interface {
	// DataSet returns a data set
	DataSet() DataSet

	// Syncer returns a synchronizer with a specified name
	Syncer(name string) Synchronizer
}

// DataSet defines the data set operations
type DataSet interface {
	State() UID
	// Get gets data according to UID
	Get(uid UID) (*Item, error)

	// Add adds data items
	Add(items ...Item) error

	// Del deletes data according to UIDs
	Del(uids ...UID) error

	// Sync syncs data according to manifest and items
	Sync(manifest Manifest, items []Item, callback func(Item) error) error
}

// Synchronizer defines the synchronizer operations
type Synchronizer interface {
	// Add adds UIDs to sync set
	Add(uids ...UID) error

	// Del deletes UIDs from sync set
	Del(uids ...UID) error

	// Manifest gets a manifest that needs to be synchronized according to the UID
	Manifest(uid UID) (Manifest, error)

	// Data gets the data items to be synchronized according to the manifest
	Data(Manifest) ([]Item, error)
}

// Item defines the data item
type Item struct {
	UID   UID
	Value []byte
}
