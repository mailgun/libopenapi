package datamodel

import (
	"context"
	"io"
	"runtime"
	"sync"

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
	KeyPtr() *K
	Value() V
	ValuePtr() *V
	Next() Pair[K, V]
}

type wrapOrderedMap[K comparable, V any] struct {
	*orderedmap.OrderedMap[K, V]
}

type wrapPair[K comparable, V any] struct {
	*orderedmap.Pair[K, V]
}

type ActionFunc[K comparable, V any] func(Pair[K, V]) error
type TranslateFunc[IN any, OUT any] func(IN) (OUT, error)
type ResultFunc[V any] func(V) error

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

func (p *wrapPair[K, V]) KeyPtr() *K {
	return &p.Pair.Key
}

func (p *wrapPair[K, V]) Value() V {
	return p.Pair.Value
}

func (p *wrapPair[K, V]) ValuePtr() *V {
	return &p.Pair.Value
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

// ForMap iterates a `Map` and calls action() on each map pair.
// action() may return `io.EOF` to break iteration.
// Safely handles nil pointer.
func ForMap[K comparable, V any](m Map[K, V], action ActionFunc[K, V]) error {
	if m == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := m.Iterate(ctx)
	for pair := range c {
		err := action(pair)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// TranslateMapParallel iterates a `Map` in parallel and calls translate()
// asynchronously.
// translate() or result() may return `io.EOF` to break iteration.
// Results are provided sequentially to result() in stable order from `Map`.
func TranslateMapParallel[K comparable, V any, RV any](m Map[K, V], translate TranslateFunc[Pair[K, V], RV], result ResultFunc[RV]) error {
	if m == nil {
		return nil
	}

	type jobStatus struct {
		done   chan struct{}
		result RV
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	concurrency := runtime.NumCPU()
	c := m.Iterate(ctx)
	jobChan := make(chan *jobStatus, concurrency)
	var reterr error
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Fan out translate jobs.
	wg.Add(1)
	go func() {
		defer func() {
			close(jobChan)
			wg.Done()
		}()
		for pair := range c {
			j := &jobStatus{
				done: make(chan struct{}),
			}
			select {
			case jobChan <- j:
			case <-ctx.Done():
				return
			}

			wg.Add(1)
			go func(pair Pair[K, V]) {
				value, err := translate(pair)
				if err != nil {
					mu.Lock()
					defer func() {
						mu.Unlock()
						wg.Done()
						cancel()
					}()
					if reterr == nil {
						reterr = err
					}
					return
				}
				j.result = value
				close(j.done)
				wg.Done()
			}(pair)
		}
	}()

	// Iterate jobChan as jobs complete.
	defer wg.Wait()
JOBLOOP:
	for j := range jobChan {
		select {
		case <-j.done:
			err := result(j.result)
			if err != nil {
				cancel()
				if err == io.EOF {
					return nil
				}
				return err
			}
		case <-ctx.Done():
			break JOBLOOP
		}
	}

	if reterr == io.EOF {
		return nil
	}
	return reterr
}
