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

import "context"

const (
	keyPrefix = "dsync_"
	keyState  = keyPrefix + "state"
)

const (
	defaultSpace    = "dsync"
	syncerSpace     = keyPrefix + "syncer"
	dataSetTmpSpace = keyPrefix + "dataset_tmp"
	dataSetSpace    = keyPrefix + "dataset"
	customSpace     = keyPrefix + "custom"
)

// Item defines the data item
type Item struct {
	UID   UID
	Value []byte
}

type KV struct {
	Key   []byte
	Value []byte
}

// StorageInterface defines storage related interfaces
type StorageInterface interface {
	// List lists all data in current space
	List(ctx context.Context, space string) ([]KV, error)

	// Get gets data according to the specified key in current space
	Get(ctx context.Context, space, key string) ([]byte, error)

	// Add adds a set of key/value pairs in current space
	Add(ctx context.Context, space, key string, value []byte) error

	// Del deletes key/value pairs according to the specified key in current space
	Del(ctx context.Context, space, key string) error
}

// Interface defines dsync core
type Interface interface {
	// DataSet returns a data set
	DataSet() DataSet

	// Syncer returns a synchronizer with a specified name
	Syncer(name string) Synchronizer
}

// Synchronizer defines the synchronizer operations
type Synchronizer interface {
	// Add adds UIDs to sync set
	Add(ctx context.Context, uids ...UID) error

	// Del deletes UIDs from sync set
	Del(ctx context.Context, uids ...UID) error

	// Manifest gets a manifest that needs to be synchronized according to the UID
	Manifest(ctx context.Context, uid UID) (Manifest, error)

	// Data gets the data items to be synchronized according to the manifest
	Data(ctx context.Context, manifest Manifest) ([]Item, error)
}

// DataSet defines the data set operations
type DataSet interface {
	// SetState sets the latest state of the dataset
	SetState(ctx context.Context, uid UID) error

	// State gets the latest state of the dataset
	State(ctx context.Context) UID

	// Get gets data according to UID
	Get(ctx context.Context, uid UID) (*Item, error)

	// Add adds data items
	Add(ctx context.Context, items ...Item) error

	// Del deletes data according to UIDs
	Del(ctx context.Context, uids ...UID) error

	// SyncManifest syncs the manifest that needs to be executed
	SyncManifest(ctx context.Context, manifest Manifest)

	// Sync syncs data according to manifest and items
	Sync(ctx context.Context, items []Item, callback ItemCallbackFunc) error
}

type ItemCallbackFunc func(context.Context, Item) error

// Customizer defines the custom data set operations
type Customizer interface {
	// SetState sets the latest state of the dataset
	SetState(ctx context.Context, key string) error

	// State gets the latest state of the dataset
	State(ctx context.Context) string

	// Get gets data according to custom key
	Get(ctx context.Context, key string) (*CustomItem, error)

	// Add adds data items
	Add(ctx context.Context, items ...CustomItem) error

	// Del deletes data according to UIDs
	Del(ctx context.Context, keys ...string) error
}

type CustomItem struct {
	Key   string
	UID   UID
	Value []byte
}

func NewCustomItem(key string, value []byte) *CustomItem {
	return &CustomItem{
		UID:   NewUID(),
		Key:   key,
		Value: value,
	}
}

// CustomInterface defines custom dsync core
type CustomInterface interface {
	// DataSet returns a custom data set
	DataSet() CustomDataSet

	// Syncer returns a custom synchronizer with a specified name
	Syncer(name string) CustomSynchronizer
}

type CustomSynchronizer interface {
	// Add adds keys to sync set
	Add(ctx context.Context, keys ...string) error

	// Del deletes keys from sync set
	Del(ctx context.Context, keys ...string) error

	// Manifest gets a manifest that needs to be synchronized according to the UID
	Manifest(ctx context.Context, uid UID) (*CustomManifest, error)

	// Data gets the data items to be synchronized according to the manifest
	Data(ctx context.Context, manifest *CustomManifest) ([]CustomItem, error)
}

type CustomDataSet interface {
	// SetState sets the latest state of the dataset
	SetState(ctx context.Context, uid UID) error

	// State gets the latest state of the dataset
	State(ctx context.Context) UID

	// Get gets data according to custom key
	Get(ctx context.Context, key string) (*CustomItem, error)

	// Add adds custom data items
	Add(ctx context.Context, items ...CustomItem) error

	// Del deletes data according to custom keys
	Del(ctx context.Context, keys ...string) error

	// SyncManifest syncs the manifest that needs to be executed
	SyncManifest(ctx context.Context, manifest Manifest)

	// Sync syncs data according to manifest and items
	Sync(ctx context.Context, items []CustomItem, callback CustomItemCallbackFunc) error
}

type CustomItemCallbackFunc func(context.Context, CustomItem) error

type CustomManifest map[string]UID
