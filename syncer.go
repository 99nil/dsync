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

	"github.com/segmentio/ksuid"
)

var (
	ErrNotFound      = errors.New("uid not found")
	ErrEmptyManifest = errors.New("empty manifest")
)

type syncer struct {
	name    string
	storage StorageInterface
}

func newSyncer(name string, storage StorageInterface) *syncer {
	return &syncer{
		name:    name,
		storage: storage,
	}
}

func (s *syncer) isExists(uids []UID, current UID) bool {
	for _, uid := range uids {
		if uid == current {
			return true
		}
	}
	return false
}

func (s *syncer) Add(ctx context.Context, uids ...UID) error {
	value, err := s.storage.Get(ctx, syncerSpace, s.name)
	if err != nil {
		return err
	}

	manifest := ksuid.AppendCompressed(value, uids...)
	return s.storage.Add(ctx, syncerSpace, s.name, manifest)
}

func (s *syncer) Del(ctx context.Context, uids ...UID) error {
	value, err := s.storage.Get(ctx, syncerSpace, s.name)
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
	return s.storage.Add(ctx, syncerSpace, s.name, manifest)
}

func (s *syncer) Manifest(ctx context.Context, uid UID) (Manifest, error) {
	value, err := s.storage.Get(ctx, syncerSpace, s.name)
	if err != nil {
		return nil, err
	}
	if len(value) == 0 {
		return nil, ErrEmptyManifest
	}

	manifest := Manifest(value)
	if uid == Nil {
		return manifest, nil
	}

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
		set = append(set, iter.KSUID)
	}

	// If there is none or only uid itself, there is nothing to synchronize.
	if len(set) < 2 {
		return manifest, ErrNotFound
	}

	manifest = ksuid.Compress(set...)
	return manifest, nil
}

func (s *syncer) Data(ctx context.Context, manifest Manifest) ([]Item, error) {
	var items []Item
	for iter := manifest.Iter(); iter.Next(); {
		current := iter.KSUID
		value, err := s.storage.Get(ctx, dataSetSpace, current.String())
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
