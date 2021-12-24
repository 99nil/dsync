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

type customSyncer struct {
	storage StorageInterface
	syncer  Synchronizer
}

func newCustomSyncer(storage StorageInterface, syncer Synchronizer) CustomSynchronizer {
	return &customSyncer{storage: storage, syncer: syncer}
}

func (s *customSyncer) getUIDByKey(ctx context.Context, key string) (UID, error) {
	value, err := s.storage.Get(ctx, customSpace, key)
	if err != nil {
		return Nil, err
	}
	return BuildUID(string(value))
}

func (s *customSyncer) Add(ctx context.Context, keys ...string) error {
	uids := make([]UID, 0, len(keys))
	for _, key := range keys {
		uid, err := s.getUIDByKey(ctx, key)
		if err != nil {
			return err
		}
		uids = append(uids, uid)
	}
	return s.syncer.Add(ctx, uids...)
}

func (s *customSyncer) Del(ctx context.Context, keys ...string) error {
	uids := make([]UID, 0, len(keys))
	for _, key := range keys {
		uid, err := s.getUIDByKey(ctx, key)
		if err != nil {
			return err
		}
		uids = append(uids, uid)
	}
	return s.syncer.Del(ctx, uids...)
}

func (s *customSyncer) Manifest(ctx context.Context, uid UID) (*CustomManifest, error) {
	// TODO
	return nil, nil
}

func (s *customSyncer) Data(ctx context.Context, manifest *CustomManifest) ([]CustomItem, error) {
	// TODO
	return nil, nil
}

type customDataSet struct {
	storage StorageInterface
	dataSet DataSet
}

func newCustomDataSet(storage StorageInterface, dataSet DataSet) *customDataSet {
	return &customDataSet{storage: storage, dataSet: dataSet}
}

func (ds *customDataSet) getUIDByKey(ctx context.Context, key string) (UID, error) {
	value, err := ds.storage.Get(ctx, customSpace, key)
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
	for _, item := range items {
		if item.UID == Nil {
			item.UID = NewUID()
		}
		// Add data first, if the association relationship fails,
		// the orphaned data will be recovered by the GC soon
		if err := ds.dataSet.Add(ctx, Item{
			UID:   item.UID,
			Value: item.Value,
		}); err != nil {
			return err
		}

		if err := ds.storage.Add(ctx, customSpace, item.Key, []byte(item.UID.String())); err != nil {
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
	ds.dataSet.SyncManifest(ctx, manifest)
}

func (ds *customDataSet) Sync(ctx context.Context, items []CustomItem, callback CustomItemCallbackFunc) error {
	ss := make([]Item, 0, len(items))
	for _, item := range items {
		ss = append(ss, Item{
			UID:   item.UID,
			Value: item.Value,
		})
	}
	// TODO
	//ds.dataSet.Sync(ctx, ss, )
	return nil
}
