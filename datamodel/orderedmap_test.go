package datamodel_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pb33f/libopenapi/datamodel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrderedMap(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		m := datamodel.NewOrderedMap[string, int]()
		assert.Equal(t, m.Len(), 0)
		assert.Nil(t, m.First())
	})

	t.Run("First()", func(t *testing.T) {
		const mapSize = 1000
		m := datamodel.NewOrderedMap[string, int]()
		for i := 0; i < mapSize; i++ {
			m.Set(fmt.Sprintf("foobar_%d", i), i)
		}
		assert.Equal(t, m.Len(), mapSize)

		for i := 0; i < mapSize; i++ {
			assert.Equal(t, i, m.GetOrZero(fmt.Sprintf("foobar_%d", i)))
		}

		var i int
		for pair := m.First(); pair != nil; pair = pair.Next() {
			assert.Equal(t, fmt.Sprintf("foobar_%d", i), pair.Key())
			assert.Equal(t, fmt.Sprintf("foobar_%d", i), *pair.KeyPtr())
			assert.Equal(t, i, pair.Value())
			assert.Equal(t, i, *pair.ValuePtr())
			i++
			require.LessOrEqual(t, i, mapSize)
		}
		assert.Equal(t, mapSize, i)
	})

	t.Run("Get()", func(t *testing.T) {
		const mapSize = 1000
		m := datamodel.NewOrderedMap[string, int]()
		for i := 0; i < mapSize; i++ {
			m.Set(fmt.Sprintf("key%d", i), 1000+i)
		}

		for i := 0; i < mapSize; i++ {
			actual, ok := m.Get(fmt.Sprintf("key%d", i))
			assert.True(t, ok)
			assert.Equal(t, 1000+i, actual)
		}

		_, ok := m.Get("bogus")
		assert.False(t, ok)
	})

	t.Run("GetOrZero()", func(t *testing.T) {
		const mapSize = 1000
		m := datamodel.NewOrderedMap[string, int]()
		for i := 0; i < mapSize; i++ {
			m.Set(fmt.Sprintf("key%d", i), 1000+i)
		}

		for i := 0; i < mapSize; i++ {
			actual := m.GetOrZero(fmt.Sprintf("key%d", i))
			assert.Equal(t, 1000+i, actual)
		}

		assert.Equal(t, 0, m.GetOrZero("bogus"))
	})

	t.Run("Iterate()", func(t *testing.T) {
		const mapSize = 10

		t.Run("Empty", func(t *testing.T) {
			m := datamodel.NewOrderedMap[string, int]()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := m.Iterate(ctx)
			for range c {
				t.Fatal("Expected no data")
			}
			requireClosed(t, c)
		})

		t.Run("Full iteration", func(t *testing.T) {
			m := datamodel.NewOrderedMap[string, int]()
			for i := 0; i < mapSize; i++ {
				m.Set(fmt.Sprintf("key%d", i), i+1000)
			}

			var i int
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := m.Iterate(ctx)
			for pair := range c {
				assert.Equal(t, fmt.Sprintf("key%d", i), pair.Key())
				assert.Equal(t, fmt.Sprintf("key%d", i), *pair.KeyPtr())
				assert.Equal(t, i+1000, pair.Value())
				assert.Equal(t, i+1000, *pair.ValuePtr())
				i++
				require.LessOrEqual(t, i, mapSize)
			}
			assert.Equal(t, mapSize, i)
			requireClosed(t, c)
		})

		t.Run("Partial iteration", func(t *testing.T) {
			m := datamodel.NewOrderedMap[string, int]()
			for i := 0; i < mapSize; i++ {
				m.Set(fmt.Sprintf("key%d", i), i+1000)
			}

			var i int
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := m.Iterate(ctx)
			for pair := range c {
				assert.Equal(t, fmt.Sprintf("key%d", i), pair.Key())
				assert.Equal(t, fmt.Sprintf("key%d", i), *pair.KeyPtr())
				assert.Equal(t, i+1000, pair.Value())
				assert.Equal(t, i+1000, *pair.ValuePtr())
				i++
				if i >= mapSize/2 {
					break
				}
			}

			cancel()
			time.Sleep(10 * time.Millisecond)
			requireClosed(t, c)
			assert.Equal(t, mapSize/2, i)
		})
	})
}

func TestMap(t *testing.T) {
	t.Run("Len()", func(t *testing.T) {
		const mapSize = 100
		m := datamodel.NewOrderedMap[string, int]()
		for i := 0; i < mapSize; i++ {
			m.Set(fmt.Sprintf("key%d", i), i+1000)
		}

		assert.Equal(t, mapSize, m.Len())
		assert.Equal(t, mapSize, datamodel.Len(m))

		t.Run("Nil pointer", func(t *testing.T) {
			var m datamodel.Map[string, int]
			assert.Zero(t, datamodel.Len(m))
		})
	})

	t.Run("ForMap()", func(t *testing.T) {
		const mapSize = 10

		t.Run("Nil pointer", func(t *testing.T) {
			var m datamodel.Map[string, int]
			err := datamodel.ForMap(m, func(_ datamodel.Pair[string, int]) error {
				return errors.New("Expected no data")
			})
			require.NoError(t, err)
		})

		t.Run("Empty", func(t *testing.T) {
			m := datamodel.NewOrderedMap[string, int]()
			err := datamodel.ForMap(m, func(_ datamodel.Pair[string, int]) error {
				return errors.New("Expected no data")
			})
			require.NoError(t, err)
		})

		t.Run("Full iteration", func(t *testing.T) {
			m := datamodel.NewOrderedMap[string, int]()
			for i := 0; i < mapSize; i++ {
				m.Set(fmt.Sprintf("key%d", i), i+1000)
			}

			var i int
			err := datamodel.ForMap(m, func(pair datamodel.Pair[string, int]) error {
				assert.Equal(t, fmt.Sprintf("key%d", i), pair.Key())
				assert.Equal(t, fmt.Sprintf("key%d", i), *pair.KeyPtr())
				assert.Equal(t, i+1000, pair.Value())
				assert.Equal(t, i+1000, *pair.ValuePtr())
				i++
				require.LessOrEqual(t, i, mapSize)
				return nil
			})
			require.NoError(t, err)
			assert.Equal(t, mapSize, i)
		})

		t.Run("Partial iteration", func(t *testing.T) {
			m := datamodel.NewOrderedMap[string, int]()
			for i := 0; i < mapSize; i++ {
				m.Set(fmt.Sprintf("key%d", i), i+1000)
			}

			var i int
			err := datamodel.ForMap(m, func(pair datamodel.Pair[string, int]) error {
				assert.Equal(t, fmt.Sprintf("key%d", i), pair.Key())
				assert.Equal(t, fmt.Sprintf("key%d", i), *pair.KeyPtr())
				assert.Equal(t, i+1000, pair.Value())
				assert.Equal(t, i+1000, *pair.ValuePtr())
				i++
				if i >= mapSize/2 {
					return io.EOF
				}
				return nil
			})
			require.NoError(t, err)
			assert.Equal(t, mapSize/2, i)
		})
	})

	t.Run("TranslateMapParallel()", func(t *testing.T) {
		const mapSize = 1000
		m := datamodel.NewOrderedMap[string, int]()
		for i := 0; i < mapSize; i++ {
			m.Set(fmt.Sprintf("key%d", i), i+1000)
		}

		var translateCounter int64
		translateFunc := func(pair datamodel.Pair[string, int]) (string, error) {
			result := fmt.Sprintf("foobar %d", pair.Value())
			atomic.AddInt64(&translateCounter, 1)
			return result, nil
		}
		var resultCounter int
		resultFunc := func(value string) error {
			assert.Equal(t, fmt.Sprintf("foobar %d", resultCounter+1000), value)
			resultCounter++
			return nil
		}
		err := datamodel.TranslateMapParallel[string, int, string](m, translateFunc, resultFunc)
		require.NoError(t, err)
		assert.Equal(t, int64(mapSize), translateCounter)
		assert.Equal(t, mapSize, resultCounter)

		t.Run("Error in translate", func(t *testing.T) {
			m := datamodel.NewOrderedMap[string, int]()
			for i := 0; i < mapSize; i++ {
				m.Set(fmt.Sprintf("key%d", i), i+1000)
			}

			var translateCounter int64
			translateFunc := func(pair datamodel.Pair[string, int]) (string, error) {
				atomic.AddInt64(&translateCounter, 1)
				return "", errors.New("Foobar")
			}
			resultFunc := func(value string) error {
				t.Fatal("Expected no call to resultFunc()")
				return nil
			}
			err := datamodel.TranslateMapParallel[string, int, string](m, translateFunc, resultFunc)
			require.ErrorContains(t, err, "Foobar")
			assert.Less(t, translateCounter, int64(mapSize))
		})

		t.Run("Error in result", func(t *testing.T) {
			m := datamodel.NewOrderedMap[string, int]()
			for i := 0; i < mapSize; i++ {
				m.Set(fmt.Sprintf("key%d", i), i+1000)
			}

			var resultCounter int
			resultFunc := func(value string) error {
				resultCounter++
				return errors.New("Foobar")
			}
			err := datamodel.TranslateMapParallel[string, int, string](m, translateFunc, resultFunc)
			require.ErrorContains(t, err, "Foobar")
			assert.Less(t, resultCounter, mapSize)
		})

		t.Run("EOF in translate", func(t *testing.T) {
			m := datamodel.NewOrderedMap[string, int]()
			for i := 0; i < mapSize; i++ {
				m.Set(fmt.Sprintf("key%d", i), i+1000)
			}

			var translateCounter int64
			translateFunc := func(pair datamodel.Pair[string, int]) (string, error) {
				atomic.AddInt64(&translateCounter, 1)
				return "", io.EOF
			}
			resultFunc := func(value string) error {
				t.Fatal("Expected no call to resultFunc()")
				return nil
			}
			err := datamodel.TranslateMapParallel[string, int, string](m, translateFunc, resultFunc)
			require.NoError(t, err)
			assert.Less(t, translateCounter, int64(mapSize))
		})

		t.Run("EOF in result", func(t *testing.T) {
			m := datamodel.NewOrderedMap[string, int]()
			for i := 0; i < mapSize; i++ {
				m.Set(fmt.Sprintf("key%d", i), i+1000)
			}

			var resultCounter int
			resultFunc := func(value string) error {
				resultCounter++
				return io.EOF
			}
			err := datamodel.TranslateMapParallel[string, int, string](m, translateFunc, resultFunc)
			require.NoError(t, err)
			assert.Less(t, resultCounter, mapSize)
		})
	})
}

func requireClosed[K comparable, V any](t *testing.T, c <-chan datamodel.Pair[K, V]) {
	select {
	case pair := <-c:
		require.Nil(t, pair, "Expected channel to be closed")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout reading channel; expected channel to be closed")
	}
}
