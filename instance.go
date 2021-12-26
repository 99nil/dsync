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

import "github.com/99nil/dsync/storage"

func New(opts ...Option) Interface {
	ins := newInstance(opts...)
	ins.dataSet = newDataSet(ins.name, ins.storage)
	return ins
}

func NewCustom(opts ...Option) CustomInterface {
	ins := newInstance(opts...)
	name := buildName("custom", ins.name)
	customIns := &customInstance{name: name, storage: ins.storage}
	customIns.dataSet = newCustomDataSet(customIns.name, customIns.storage)
	return customIns
}

type Option func(i *instance)

func WithStorageOption(storage storage.Interface) Option {
	return func(i *instance) {
		i.storage = storage
	}
}

func WithNameOption(name string) Option {
	return func(i *instance) {
		i.name = name
	}
}

type instance struct {
	name    string
	storage storage.Interface
	dataSet DataSet
}

func newInstance(opts ...Option) *instance {
	ins := &instance{}
	for _, opt := range opts {
		opt(ins)
	}
	return ins
}

func (i *instance) DataSet() DataSet {
	return i.dataSet
}

func (i *instance) Syncer(name string) Synchronizer {
	return newSyncer(i.name, name, i.storage)
}

type customInstance struct {
	name    string
	storage storage.Interface
	dataSet CustomDataSet
}

func (i *customInstance) DataSet() CustomDataSet {
	return i.dataSet
}

func (i *customInstance) Syncer(name string) CustomSynchronizer {
	return newCustomSyncer(i.name, name, i.storage)
}
