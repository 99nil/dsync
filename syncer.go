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

var ErrNotFound = errors.New("uid not found")

type syncer struct {
	name    string
	storage StorageInterface
}

func (s *syncer) buildKey() []byte {
	return []byte(s.name)
}

func (s *syncer) isExists(uids []UID, current UID) bool {
	for _, uid := range uids {
		if uid == current {
			return true
		}
	}
	return false
}

func (s *syncer) Add(uids ...UID) error {
	key := s.buildKey()
	value, err := s.storage.Get(syncerSpace, key)
	if err != nil {
		return err
	}

	manifest := ksuid.AppendCompressed(value, uids...)
	return s.storage.Add(syncerSpace, key, manifest)
}

func (s *syncer) Del(uids ...UID) error {
	key := s.buildKey()
	value, err := s.storage.Get(syncerSpace, key)
	if err != nil {
		return err
	}

	var set []UID
	manifest := Manifest(value)
	for iter := manifest.Iter(); iter.Next(); {
		current := iter.KSUID
		if s.isExists(uids, current) {
			continue
		}
		set = append(set, current)
	}

	manifest = ksuid.Compress(set...)
	return s.storage.Add(syncerSpace, key, manifest)
}

func (s *syncer) Manifest(uid UID) (Manifest, error) {
	key := s.buildKey()
	value, err := s.storage.Get(syncerSpace, key)
	if err != nil {
		return nil, err
	}

	var (
		set    []UID
		exists bool
	)
	manifest := Manifest(value)
	for iter := manifest.Iter(); iter.Next(); {
		current := iter.KSUID
		if current == uid {
			exists = true
		}
		if !exists {
			continue
		}
		set = append(set, current)
	}
	if len(set) == 0 {
		return manifest, ErrNotFound
	}

	manifest = ksuid.Compress(set...)
	return manifest, nil
}

func (s *syncer) Data(manifest Manifest) ([]Item, error) {
	var items []Item
	for iter := manifest.Iter(); iter.Next(); {
		current := iter.KSUID
		value, err := s.storage.Get(dataSetSpace, current.Bytes())
		if err != nil {
			return nil, err
		}
		items = append(items, Item{
			UID:   current,
			Value: value,
		})
	}
	return items, nil
}
