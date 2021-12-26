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

import (
	"context"
	"errors"
	"sync"

	"github.com/segmentio/ksuid"
)

var (
	ErrDataNotMatch  = errors.New("data not match")
	ErrUnexpectState = errors.New("unexpect state")
)

type dataSet struct {
	storage  StorageInterface
	mux      sync.Mutex
	manifest Manifest
	state    UID
}

func newDataSet(storage StorageInterface) *dataSet {
	return &dataSet{storage: storage}
}

func (ds *dataSet) SetState(ctx context.Context, uid UID) error {
	if err := ds.storage.Add(ctx, defaultSpace, keyState, []byte(uid.String())); err != nil {
		return err
	}
	ds.state = uid
	return nil
}

func (ds *dataSet) State(ctx context.Context) UID {
	if ds.state != Nil {
		return ds.state
	}

	value, err := ds.storage.Get(ctx, defaultSpace, keyState)
	if err != nil {
		return ds.state
	}

	uid, err := BuildUID(string(value))
	if err != nil {
		return ds.state
	}
	ds.state = uid
	return ds.state
}

func (ds *dataSet) Get(ctx context.Context, uid UID) (*Item, error) {
	value, err := ds.storage.Get(ctx, dataSetSpace, uid.String())
	if err != nil {
		return nil, err
	}
	return &Item{
		UID:   uid,
		Value: value,
	}, nil
}

func (ds *dataSet) Add(ctx context.Context, items ...Item) error {
	if len(items) == 0 {
		return nil
	}

	state := ds.State(ctx)
	for _, item := range items {
		if err := ds.storage.Add(ctx, dataSetSpace, item.UID.String(), item.Value); err != nil {
			return err
		}

		if ksuid.Compare(state, item.UID) > -1 {
			continue
		}
		if err := ds.SetState(ctx, item.UID); err != nil {
			return err
		}
	}
	return nil
}

func (ds *dataSet) Del(ctx context.Context, uids ...UID) error {
	for _, uid := range uids {
		if err := ds.storage.Del(ctx, dataSetSpace, uid.String()); err != nil {
			return err
		}
	}
	return nil
}

func (ds *dataSet) SyncManifest(ctx context.Context, manifest Manifest) {
	ds.mux.Lock()
	defer ds.mux.Unlock()

	state := ds.State(ctx)
	if state != Nil {
		var (
			set    []UID
			exists bool
		)
		for iter := manifest.Iter(); iter.Next(); {
			if iter.KSUID == state {
				exists = true
			}
			if exists {
				set = append(set, iter.KSUID)
			}
		}
		manifest = ksuid.Compress(set...)
	}
	ds.manifest = manifest
}

func (ds *dataSet) Sync(ctx context.Context, items []Item, callback ItemCallbackFunc) error {
	if len(items) == 0 {
		return nil
	}
	state := ds.State(ctx)

	var count int
	// Store items in tmp space first,
	// and use items in subsequent synchronization to prevent the sequence of data from affecting synchronization.
	for _, item := range items {
		if ksuid.Compare(state, item.UID) > -1 {
			continue
		}
		if err := ds.storage.Add(ctx, tmpDataSetSpace, item.UID.String(), item.Value); err != nil {
			return err
		}
		count++
	}
	if count == 0 {
		return nil
	}

	// In order to ensure the consistency of the manifest,
	// it needs to be locked before this.
	ds.mux.Lock()
	defer ds.mux.Unlock()

	var match bool
	for i, iter := 0, ds.manifest.Iter(); iter.Next(); i++ {
		current := iter.KSUID
		state := ds.State(ctx)

		// When state is Nil, directly synchronize data
		if state == Nil {
			match = true
		}

		if !match {
			// When the corresponding state is found in the manifest,
			// start synchronization from the next.
			if state == current {
				match = true
			}
			continue
		}

		value, err := ds.storage.Get(ctx, tmpDataSetSpace, current.String())
		if err != nil {
			return err
		}
		if value == nil {
			return ErrDataNotMatch
		}

		item := Item{UID: current, Value: value}
		if err := ds.Add(ctx, item); err != nil {
			return err
		}
		if callback != nil {
			if err := callback(ctx, item); err != nil {
				return err
			}
		}

		// Try to delete the synchronized data in the tmp space.
		// If the deletion fails, no verification is required,
		// and it can be reclaimed by the GC later.
		_ = ds.storage.Del(ctx, tmpDataSetSpace, current.String())
	}
	return nil
}
