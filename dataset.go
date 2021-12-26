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
	"encoding/json"
	"errors"
	"sync"

	"github.com/99nil/dsync/storage"

	"github.com/segmentio/ksuid"
)

var (
	ErrDataNotMatch  = errors.New("data not match")
	ErrUnexpectState = errors.New("unexpect state")
)

type dataSet struct {
	mux      sync.Mutex
	manifest Manifest
	state    UID

	defaultOperation OperateInterface
	dataSetOperation OperateInterface
	tmpOperation     OperateInterface
}

func newDataSet(insName string, storage storage.Interface) *dataSet {
	ds := new(dataSet)
	ds.defaultOperation = newSpaceOperation(buildName(prefix, insName), storage)
	ds.dataSetOperation = newSpaceOperation(buildName(prefix, "dataset", insName), storage)
	ds.tmpOperation = newSpaceOperation(buildName(prefix, "tmp", insName), storage)
	return ds
}

func (ds *dataSet) SetState(ctx context.Context, uid UID) error {
	if err := ds.defaultOperation.Add(ctx, keyState, []byte(uid.String())); err != nil {
		return err
	}
	ds.state = uid
	return nil
}

func (ds *dataSet) State(ctx context.Context) UID {
	if ds.state != Nil {
		return ds.state
	}

	value, err := ds.defaultOperation.Get(ctx, keyState)
	if err != nil {
		return ds.state
	}

	uid, err := BuildUIDFromBytes(value)
	if err != nil {
		return ds.state
	}
	ds.state = uid
	return ds.state
}

func (ds *dataSet) Get(ctx context.Context, uid UID) (*Item, error) {
	value, err := ds.dataSetOperation.Get(ctx, uid.String())
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
		// When adding data in batches, the order may not be guaranteed,
		// so perform the addition first, and then determine the latest state.
		if err := ds.dataSetOperation.Add(ctx, item.UID.String(), item.Value); err != nil {
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
		if err := ds.dataSetOperation.Del(ctx, uid.String()); err != nil {
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
		if err := ds.tmpOperation.Add(ctx, item.UID.String(), item.Value); err != nil {
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
		uid := iter.KSUID
		current := uid.String()
		state := ds.State(ctx)

		// When state is Nil, directly synchronize data
		if state == Nil {
			match = true
		}
		if !match {
			// When the corresponding state is found in the manifest,
			// start synchronization from the next.
			if state == uid {
				match = true
			}
			continue
		}

		value, err := ds.tmpOperation.Get(ctx, current)
		if err != nil {
			return err
		}
		if value == nil {
			return ErrDataNotMatch
		}

		item := Item{UID: uid, Value: value}
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
		_ = ds.tmpOperation.Del(ctx, current)
	}
	return nil
}

type customDataSet struct {
	mux      sync.Mutex
	manifest Manifest
	state    UID

	defaultOperation OperateInterface
	dataSetOperation OperateInterface
	tmpOperation     OperateInterface
	customOperation  OperateInterface
}

func newCustomDataSet(insName string, storage storage.Interface) *customDataSet {
	ds := new(customDataSet)
	ds.defaultOperation = newSpaceOperation(buildName(prefix, insName), storage)
	ds.dataSetOperation = newSpaceOperation(buildName(prefix, "dataset", insName), storage)
	ds.tmpOperation = newSpaceOperation(buildName(prefix, "tmp", insName), storage)
	ds.customOperation = newSpaceOperation(buildName(prefix, "relate", insName), storage)
	return ds
}

func (ds *customDataSet) getUIDByKey(ctx context.Context, key string) (UID, error) {
	value, err := ds.customOperation.Get(ctx, key)
	if err != nil {
		return Nil, err
	}
	return BuildUIDFromBytes(value)
}

func (ds *customDataSet) SetState(ctx context.Context, uid UID) error {
	if err := ds.defaultOperation.Add(ctx, keyState, []byte(uid.String())); err != nil {
		return err
	}
	ds.state = uid
	return nil
}

func (ds *customDataSet) State(ctx context.Context) UID {
	if ds.state != Nil {
		return ds.state
	}

	value, err := ds.defaultOperation.Get(ctx, keyState)
	if err != nil {
		return ds.state
	}

	uid, err := BuildUIDFromBytes(value)
	if err != nil {
		return ds.state
	}
	ds.state = uid
	return ds.state
}

func (ds *customDataSet) Get(ctx context.Context, key string) (*CustomItem, error) {
	uid, err := ds.getUIDByKey(ctx, key)
	if err != nil {
		return nil, err
	}
	value, err := ds.dataSetOperation.Get(ctx, uid.String())
	if err != nil {
		return nil, err
	}
	return &CustomItem{
		Key:   key,
		UID:   uid,
		Value: value,
	}, nil
}

func (ds *customDataSet) Add(ctx context.Context, items ...CustomItem) error {
	if len(items) == 0 {
		return nil
	}

	uid := NewUID()
	state := ds.State(ctx)
	for _, item := range items {
		if item.UID == Nil {
			item.UID = uid
			uid = uid.Next()
		}

		// Add data first, if the association relationship fails,
		// the orphaned data will be recovered by the GC soon
		if err := ds.dataSetOperation.Add(ctx, item.UID.String(), item.Value); err != nil {
			return err
		}
		if err := ds.customOperation.Add(ctx, item.Key, []byte(item.UID.String())); err != nil {
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

func (ds *customDataSet) Del(ctx context.Context, keys ...string) error {
	for _, key := range keys {
		uid, err := ds.getUIDByKey(ctx, key)
		if err != nil {
			return err
		}
		if err := ds.dataSetOperation.Del(ctx, uid.String()); err != nil {
			return err
		}
	}
	return nil
}

func (ds *customDataSet) SyncManifest(ctx context.Context, manifest Manifest) {
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

func (ds *customDataSet) Sync(ctx context.Context, items []CustomItem, callback CustomItemCallbackFunc) error {
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
		if err := ds.tmpOperation.AddData(ctx, item.UID.String(), item); err != nil {
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
		uid := iter.KSUID
		current := uid.String()
		state := ds.State(ctx)

		// When state is Nil, directly synchronize data
		if state == Nil {
			match = true
		}
		if !match {
			// When the corresponding state is found in the manifest,
			// start synchronization from the next.
			if state == uid {
				match = true
			}
			continue
		}

		value, err := ds.tmpOperation.Get(ctx, current)
		if err != nil {
			return err
		}
		if value == nil {
			return ErrDataNotMatch
		}
		var item CustomItem
		if err := json.Unmarshal(value, &item); err != nil {
			return err
		}

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
		_ = ds.tmpOperation.Del(ctx, current)
	}
	return nil
}
