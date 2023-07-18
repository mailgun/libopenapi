package datamodel

import (
	"context"

	orderedmap "github.com/wk8/go-ordered-map/v2"
)

type Map[K comparable, V any] interface {
	Lengthiness
	Get(K) (V, bool)
	GetOrZero(K) V
	Set(K, V) (V, bool)
	Delete(K) (V, bool)
	First() Pair[K, V]
	Iterate(ctx context.Context) <-chan Pair[K, V]
}

type Lengthiness interface {
	Len() int
}

type Pair[K comparable, V any] interface {
	Key() K
	Value() V
	Next() Pair[K, V]
}

type wrapOrderedMap[K comparable, V any] struct {
	*orderedmap.OrderedMap[K, V]
}

type wrapPair[K comparable, V any] struct {
	*orderedmap.Pair[K, V]
}

// NewOrderedMap creates an ordered map generic object.
func NewOrderedMap[K comparable, V any]() Map[K, V] {
	return &wrapOrderedMap[K, V]{
		OrderedMap: orderedmap.New[K, V](),
	}
}

func (o *wrapOrderedMap[K, V]) GetOrZero(k K) V {
	v, ok := o.OrderedMap.Get(k)
	if !ok {
		var zero V
		return zero
	}
	return v
}

func (o *wrapOrderedMap[K, V]) First() Pair[K, V] {
	pair := o.OrderedMap.Oldest()
	if pair == nil {
		return nil
	}
	return &wrapPair[K, V]{
		Pair: pair,
	}
}

// Iterate the map in order.  Be sure to iterate to end or cancel the context
// when done to release goroutine.
func (o *wrapOrderedMap[K, V]) Iterate(ctx context.Context) <-chan Pair[K, V] {
	c := make(chan Pair[K, V])
	go func() {
		defer close(c)
		for pair := o.First(); pair != nil; pair = pair.Next() {
			select {
			case c <- pair:
			case <-ctx.Done():
				return
			}
		}
	}()
	return c
}

func (p *wrapPair[K, V]) Next() Pair[K, V] {
	next := p.Pair.Next()
	if next == nil {
		return nil
	}
	return &wrapPair[K, V]{
		Pair: next,
	}
}

func (p *wrapPair[K, V]) Key() K {
	return p.Pair.Key
}

func (p *wrapPair[K, V]) Value() V {
	return p.Pair.Value
}

// Len returns the length of a container implementing a `Len()` method.
// Safely returns zero on nil pointer.
func Len(l Lengthiness) int {
	if l == nil {
		return 0
	}
	return l.Len()
}

// ToOrderedMap converts map built-in to OrderedMap.
func ToOrderedMap[K comparable, V any](m map[K]V) Map[K, V] {
	om := NewOrderedMap[K, V]()
	for k, v := range m {
		om.Set(k, v)
	}
	return om
}

// IterateMap returns a channel to iterate a `Map`.
// Be sure to iterate to end or cancel the context when done to release
// goroutine.
// Safely returns a closed channel on nil pointer.
func IterateMap[K comparable, V any](ctx context.Context, om Map[K, V]) <-chan Pair[K, V] {
	c := make(chan Pair[K, V])
	if om == nil {
		close(c)
		return c
	}
	return om.Iterate(ctx)
}
