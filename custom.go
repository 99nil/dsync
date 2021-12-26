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
	"sync"

	"github.com/segmentio/ksuid"
)

type customSyncer struct {
	name    string
	storage StorageInterface
	syncer  Synchronizer
}

func newCustomSyncer(name string, storage StorageInterface, syncer Synchronizer) CustomSynchronizer {
	return &customSyncer{name: name, storage: storage, syncer: syncer}
}

func (s *customSyncer) getKeyMap(ctx context.Context, keys ...string) (map[string]UID, error) {
	keyMap := make(map[string]UID)
	for _, key := range keys {
		value, err := s.storage.Get(ctx, customDataSetSpace, key)
		if err != nil {
			return nil, err
		}
		uid, err := BuildUID(string(value))
		if err != nil {
			return nil, err
		}
		keyMap[key] = uid
	}
	return keyMap, nil
}

func (s *customSyncer) getCustomManifest(ctx context.Context) (map[string]UID, error) {
	value, err := s.storage.Get(ctx, customSyncerSpace, s.name)
	if err != nil {
		return nil, err
	}
	var customManifest map[string]UID
	if err := json.Unmarshal(value, &customManifest); err != nil {
		return nil, err
	}
	return customManifest, nil
}

func (s *customSyncer) reverseCustomManifest(customManifest map[string]UID) map[UID]string {
	result := make(map[UID]string, len(customManifest))
	for k, v := range customManifest {
		result[v] = k
	}
	return result
}

func (s *customSyncer) Add(ctx context.Context, keys ...string) error {
	keyMap, err := s.getKeyMap(ctx, keys...)
	if err != nil {
		return err
	}
	if len(keyMap) == 0 {
		return nil
	}

	customManifest, err := s.getCustomManifest(ctx)
	if err != nil {
		return err
	}

	for key, uid := range keyMap {
		customManifest[key] = uid
	}
	return s.setManifest(ctx, customManifest)
}

func (s *customSyncer) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	customManifest, err := s.getCustomManifest(ctx)
	if err != nil {
		return err
	}

	for _, key := range keys {
		delete(customManifest, key)
	}
	return s.setManifest(ctx, customManifest)
}

func (s *customSyncer) setManifest(ctx context.Context, customManifest map[string]UID) error {
	b, err := json.Marshal(customManifest)
	if err != nil {
		return err
	}
	return s.storage.Add(ctx, customSyncerSpace, s.name, b)
}

func (s *customSyncer) Manifest(ctx context.Context, uid UID) (Manifest, error) {
	customManifest, err := s.getCustomManifest(ctx)
	if err != nil {
		return nil, err
	}
	if len(customManifest) == 0 {
		return nil, ErrEmptyManifest
	}

	var manifest Manifest
	for _, v := range customManifest {
		manifest = ksuid.AppendCompressed(manifest, v)
	}
	if uid == Nil {
		return manifest, nil
	}

	reverse := s.reverseCustomManifest(customManifest)
	customManifest = make(map[string]UID)

	var (
		set    []UID
		exists bool
	)
	manifest = ksuid.AppendCompressed(manifest, uid)
	for iter := manifest.Iter(); iter.Next(); {
		if iter.KSUID == uid {
			exists = true
		}
		if !exists {
			continue
		}

		key, ok := reverse[iter.KSUID]
		if !ok {
			return nil, ErrUnexpectState
		}
		customManifest[key] = iter.KSUID
		set = append(set, iter.KSUID)
	}

	// If there is none or only uid itself, there is nothing to synchronize.
	if len(set) < 2 {
		_ = s.setManifest(ctx, nil)
		return nil, ErrEmptyManifest
	}
	if err := s.setManifest(ctx, customManifest); err != nil {
		return nil, err
	}
	manifest = ksuid.Compress(set...)
	return manifest, nil
}

func (s *customSyncer) Data(ctx context.Context, manifest Manifest) ([]CustomItem, error) {
	customManifest, err := s.getCustomManifest(ctx)
	if err != nil {
		return nil, err
	}
	if len(customManifest) == 0 {
		return nil, nil
	}

	reverse := s.reverseCustomManifest(customManifest)

	items := make([]CustomItem, 0, len(reverse))
	for iter := manifest.Iter(); iter.Next(); {
		current := iter.KSUID

		key, ok := reverse[current]
		if !ok {
			continue
		}
		value, err := s.storage.Get(ctx, customDataSetSpace, current.String())
		if err != nil {
			return nil, err
		}
		items = append(items, CustomItem{
			Key:   key,
			UID:   current,
			Value: value,
		})
	}
	return items, nil
}

type customDataSet struct {
	storage  StorageInterface
	dataSet  DataSet
	manifest Manifest
	mux      sync.Mutex
}

func newCustomDataSet(storage StorageInterface, dataSet DataSet) *customDataSet {
	return &customDataSet{storage: storage, dataSet: dataSet}
}

func (ds *customDataSet) getUIDByKey(ctx context.Context, key string) (UID, error) {
	value, err := ds.storage.Get(ctx, customDataSetSpace, key)
	if err != nil {
		return Nil, err
	}
	return BuildUID(string(value))
}

func (ds *customDataSet) SetState(ctx context.Context, uid UID) error {
	return ds.dataSet.SetState(ctx, uid)
}

func (ds *customDataSet) State(ctx context.Context) UID {
	return ds.dataSet.State(ctx)
}

func (ds *customDataSet) Get(ctx context.Context, key string) (*CustomItem, error) {
	uid, err := ds.getUIDByKey(ctx, key)
	if err != nil {
		return nil, err
	}
	item, err := ds.dataSet.Get(ctx, uid)
	if err != nil {
		return nil, err
	}
	return &CustomItem{
		Key:   key,
		UID:   item.UID,
		Value: item.Value,
	}, nil
}

func (ds *customDataSet) Add(ctx context.Context, items ...CustomItem) error {
	uid := NewUID()
	for _, item := range items {
		if item.UID == Nil {
			item.UID = uid
			uid = uid.Next()
		}
		// Add data first, if the association relationship fails,
		// the orphaned data will be recovered by the GC soon
		if err := ds.dataSet.Add(ctx, Item{
			UID:   item.UID,
			Value: item.Value,
		}); err != nil {
			return err
		}

		if err := ds.storage.Add(ctx, customDataSetSpace, item.Key, []byte(item.UID.String())); err != nil {
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
		if err := ds.dataSet.Del(ctx, uid); err != nil {
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
	ss := make([]Item, 0, len(items))
	for _, item := range items {
		ss = append(ss, Item{
			UID:   item.UID,
			Value: item.Value,
		})
	}

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
		b, err := json.Marshal(item)
		if err != nil {
			return err
		}
		if err := ds.storage.Add(ctx, customTmpDataSetSpace, item.UID.String(), b); err != nil {
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

		value, err := ds.storage.Get(ctx, customTmpDataSetSpace, current.String())
		if err != nil {
			return err
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
		_ = ds.storage.Del(ctx, customTmpDataSetSpace, current.String())
	}
	return nil
}
