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

	"github.com/99nil/dsync/storage"

	"github.com/segmentio/ksuid"
)

var (
	ErrEmptyManifest = errors.New("empty manifest")
)

type syncer struct {
	name             string
	syncerOperation  OperateInterface
	dataSetOperation OperateInterface
}

func newSyncer(insName string, name string, storage storage.Interface) *syncer {
	s := &syncer{name: name}
	s.syncerOperation = newSpaceOperation(buildName(prefix, "syncer", insName), storage)
	s.dataSetOperation = newSpaceOperation(buildName(prefix, "dataset", insName), storage)
	return s
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
	value, err := s.syncerOperation.Get(ctx, s.name)
	if err != nil {
		return err
	}

	manifest := ksuid.AppendCompressed(value, uids...)
	return s.syncerOperation.Add(ctx, s.name, manifest)
}

func (s *syncer) Del(ctx context.Context, uids ...UID) error {
	value, err := s.syncerOperation.Get(ctx, s.name)
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
	return s.syncerOperation.Add(ctx, s.name, manifest)
}

func (s *syncer) setManifest(ctx context.Context, manifest Manifest) error {
	return s.syncerOperation.Add(ctx, s.name, manifest)
}

func (s *syncer) Manifest(ctx context.Context, uid UID) (Manifest, error) {
	value, err := s.syncerOperation.Get(ctx, s.name)
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
		_ = s.setManifest(ctx, nil)
		return nil, ErrEmptyManifest
	}

	manifest = ksuid.Compress(set...)
	if err := s.setManifest(ctx, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

func (s *syncer) Data(ctx context.Context, manifest Manifest) ([]Item, error) {
	var items []Item
	for iter := manifest.Iter(); iter.Next(); {
		value, err := s.dataSetOperation.Get(ctx, iter.KSUID.String())
		if err != nil {
			return nil, err
		}
		items = append(items, Item{
			UID:   iter.KSUID,
			Value: value,
		})
	}
	return items, nil
}

type customSyncer struct {
	name             string
	syncerOperation  OperateInterface
	dataSetOperation OperateInterface
	customOperation  OperateInterface
}

func newCustomSyncer(insName string, name string, storage storage.Interface) CustomSynchronizer {
	s := &customSyncer{name: name}
	s.syncerOperation = newSpaceOperation(buildName(prefix, "syncer", insName), storage)
	s.dataSetOperation = newSpaceOperation(buildName(prefix, "dataset", insName), storage)
	s.customOperation = newSpaceOperation(buildName(prefix, "custom", insName), storage)
	return s
}

func (s *customSyncer) getKeyMap(ctx context.Context, keys ...string) (map[string]UID, error) {
	keyMap := make(map[string]UID)
	for _, key := range keys {
		value, err := s.customOperation.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		uid, err := BuildUIDFromBytes(value)
		if err != nil {
			return nil, err
		}
		keyMap[key] = uid
	}
	return keyMap, nil
}

func (s *customSyncer) getCustomManifest(ctx context.Context) (CustomManifest, error) {
	value, err := s.syncerOperation.Get(ctx, s.name)
	if err != nil {
		return nil, err
	}
	var customManifest CustomManifest
	if err := json.Unmarshal(value, &customManifest); err != nil {
		return nil, err
	}
	return customManifest, nil
}

func (s *customSyncer) setManifest(ctx context.Context, customManifest map[string]UID) error {
	b, err := json.Marshal(customManifest)
	if err != nil {
		return err
	}
	return s.syncerOperation.Add(ctx, s.name, b)
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

	reverse := customManifest.reverse()
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

	reverse := customManifest.reverse()

	items := make([]CustomItem, 0, len(reverse))
	for iter := manifest.Iter(); iter.Next(); {
		current := iter.KSUID
		key, ok := reverse[current]
		if !ok {
			continue
		}

		value, err := s.dataSetOperation.Get(ctx, current.String())
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
