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

type Option func(i *instance)

func WithStorageOption(storage StorageInterface) Option {
	return func(i *instance) {
		i.storage = storage
	}
}

type instance struct {
	storage StorageInterface
}

func New(opts ...Option) Interface {
	ins := &instance{}
	for _, opt := range opts {
		opt(ins)
	}
	return ins
}

func (i *instance) DataSet() DataSet {
	return newDataSet(i.storage)
}

func (i *instance) Syncer(name string) Synchronizer {
	return newSyncer(name, i.storage)
}

type customInstance struct {
	ins *instance
}

func NewCustom(opts ...Option) CustomInterface {
	ins := &instance{}
	for _, opt := range opts {
		opt(ins)
	}
	return &customInstance{ins: ins}
}

func (i *customInstance) DataSet() CustomDataSet {
	return newCustomDataSet(i.ins.storage, i.ins.DataSet())
}

func (i *customInstance) Syncer(name string) CustomSynchronizer {
	return newCustomSyncer(i.ins.storage, i.ins.Syncer(name))
}
