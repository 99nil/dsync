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
)

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
	// SetState ...
	SetState(ctx context.Context, uid UID) error
	// State ...
	State(ctx context.Context) UID
	// Get gets data according to UID
	Get(ctx context.Context, uid UID) (*Item, error)

	// Add adds data items
	Add(ctx context.Context, items ...Item) error

	// Del deletes data according to UIDs
	Del(ctx context.Context, uids ...UID) error

	// Sync syncs data according to manifest and items
	Sync(ctx context.Context, manifest Manifest, items []Item, callback ItemCallbackFunc) error
}

type ItemCallbackFunc func(context.Context, Item) error

// Item defines the data item
type Item struct {
	UID   UID
	Value []byte
}
