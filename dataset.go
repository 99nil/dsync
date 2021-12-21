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

import "errors"

var (
	ErrDataNotMatch  = errors.New("data not match")
	ErrUnexpectState = errors.New("unexpect state")
)

type dataSet struct {
	storage StorageInterface
}

func (ds *dataSet) State() UID {
	value, err := ds.storage.Get([]byte(keyState))
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
	value, err := ds.storage.Get(uid.Bytes())
	if err != nil {
		return nil, err
	}
	return &Item{
		UID:   uid,
		Value: value,
	}, nil
}

func (ds *dataSet) Add(items ...Item) error {
	for _, item := range items {
		if err := ds.storage.Add(item.UID.Bytes(), item.Value); err != nil {
			return err
		}
	}
	return nil
}

func (ds *dataSet) Del(uids ...UID) error {
	for _, uid := range uids {
		if err := ds.storage.Del(uid.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func (ds *dataSet) Sync(manifest Manifest, items []Item) error {
	state := ds.State()
	for i, iter := 0, manifest.Iter(); iter.Next(); i++ {
		if i == 0 && state != Nil {
			if state == iter.KSUID {
				continue
			}
			return ErrUnexpectState
		}

		var exists bool
		for _, item := range items {
			if iter.KSUID != item.UID {
				continue
			}
			if err := ds.Add(item); err != nil {
				return err
			}
			exists = true
			break
		}
		if !exists {
			return ErrDataNotMatch
		}
	}
	return nil
}
