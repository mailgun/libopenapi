package datamodel_test

import (
	"context"
	"fmt"
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
			assert.Equal(t, i, pair.Value())
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
				assert.Equal(t, i+1000, pair.Value())
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
				assert.Equal(t, i+1000, pair.Value())
				i++
				if i >= mapSize/2 {
					break
				}
			}

			cancel()
			time.Sleep(10 * time.Millisecond)
			requireClosed(t, c)
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

	t.Run("IterateMap()", func(t *testing.T) {
		const mapSize = 10

		t.Run("Nil pointer", func(t *testing.T) {
			var m datamodel.Map[string, int]
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := datamodel.IterateMap(ctx, m)
			for range c {
				t.Fatal("Expected no data")
			}
			requireClosed(t, c)
		})

		t.Run("Empty", func(t *testing.T) {
			m := datamodel.NewOrderedMap[string, int]()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := datamodel.IterateMap(ctx, m)
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
			c := datamodel.IterateMap(ctx, m)
			for pair := range c {
				assert.Equal(t, fmt.Sprintf("key%d", i), pair.Key())
				assert.Equal(t, i+1000, pair.Value())
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
			c := datamodel.IterateMap(ctx, m)
			for pair := range c {
				assert.Equal(t, fmt.Sprintf("key%d", i), pair.Key())
				assert.Equal(t, i+1000, pair.Value())
				i++
				if i >= mapSize/2 {
					break
				}
			}

			cancel()
			time.Sleep(10 * time.Millisecond)
			requireClosed(t, c)
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
