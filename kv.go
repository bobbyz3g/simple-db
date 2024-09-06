package simpledb

import (
	"sync"

	"github.com/google/btree"
)

type KV[T any] struct {
	lock *sync.RWMutex

	tree *btree.BTreeG[item[T]]
}

type item[T any] struct {
	Key   string
	Value T
}

func NewKV[T any]() *KV[T] {
	return &KV[T]{
		lock: &sync.RWMutex{},
		tree: btree.NewG(2, func(a, b item[T]) bool {
			return a.Key < b.Key
		}),
	}
}

func (kv *KV[T]) Get(key string) (_ T, _ bool) {
	kv.lock.RLock()

	val, ok := kv.tree.Get(item[T]{Key: key})
	kv.lock.RUnlock()
	if !ok {
		return
	}
	return val.Value, true
}

func (kv *KV[T]) Set(key string, value T) {
	kv.lock.Lock()
	kv.tree.ReplaceOrInsert(item[T]{Key: key, Value: value})
	kv.lock.Unlock()
}
