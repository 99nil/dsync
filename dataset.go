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
	"errors"

	"github.com/segmentio/ksuid"
)

var (
	ErrDataNotMatch  = errors.New("data not match")
	ErrUnexpectState = errors.New("unexpect state")
)

type dataSet struct {
	storage StorageInterface
}

func (ds *dataSet) setState(uid UID) error {
	return ds.storage.Add(defaultSpace, []byte(keyState), uid.Bytes())
}

func (ds *dataSet) State() UID {
	value, err := ds.storage.Get(defaultSpace, []byte(keyState))
	if err != nil {
		return Nil
	}
	uid, err := BuildUIDFromBytes(value)
	if err != nil {
		return Nil
	}
	return uid
}

func (ds *dataSet) Get(uid UID) (*Item, error) {
	value, err := ds.storage.Get(dataSetSpace, uid.Bytes())
	if err != nil {
		return nil, err
	}
	return &Item{
		UID:   uid,
		Value: value,
	}, nil
}

func (ds *dataSet) Add(items ...Item) error {
	if len(items) == 0 {
		return nil
	}

	state := ds.State()
	for _, item := range items {
		key := item.UID.Bytes()
		if err := ds.storage.Add(dataSetSpace, key, item.Value); err != nil {
			return err
		}

		if ksuid.Compare(state, item.UID) > 0 {
			continue
		}
		if err := ds.setState(item.UID); err != nil {
			return err
		}
	}
	return nil
}

func (ds *dataSet) Del(uids ...UID) error {
	for _, uid := range uids {
		if err := ds.storage.Del(dataSetSpace, uid.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func (ds *dataSet) tmpList() ([]Item, error) {
	list, err := ds.storage.List(dataSetTmpSpace)
	if err != nil {
		return nil, err
	}

	var items []Item
	for _, v := range list {
		uid, err := BuildUIDFromBytes(v.Key)
		if err != nil {
			return nil, err
		}
		items = append(items, Item{
			UID:   uid,
			Value: v.Value,
		})
	}
	return items, nil
}

func (ds *dataSet) Sync(manifest Manifest, items []Item, callback func(Item) error) error {
	if len(items) == 0 {
		return nil
	}

	// Store items in tmp space first,
	// and use items in subsequent synchronization to prevent the sequence of data from affecting synchronization.
	for _, item := range items {
		if err := ds.storage.Add(dataSetTmpSpace, item.UID.Bytes(), item.Value); err != nil {
			return err
		}
	}

	// Take out all the items in the tmp space and start synchronization.
	list, err := ds.tmpList()
	if err != nil {
		return err
	}

	state := ds.State()
	for i, iter := 0, manifest.Iter(); iter.Next(); i++ {
		current := iter.KSUID
		if i == 0 && state != Nil {
			if state == current {
				continue
			}
			return ErrUnexpectState
		}

		var exists bool
		for _, item := range list {
			if current != item.UID {
				continue
			}
			if err := ds.Add(item); err != nil {
				return err
			}
			if callback != nil {
				if err := callback(item); err != nil {
					return err
				}
			}
			exists = true

			// Try to delete the synchronized data in the tmp space.
			// If the deletion fails, no verification is required,
			// and it can be reclaimed by the GC later.
			_ = ds.storage.Del(dataSetTmpSpace, item.UID.Bytes())
			break
		}
		if !exists {
			return ErrDataNotMatch
		}
	}
	return nil
}
