package resolver

import (
	"context"
	"sync"
	"sync/atomic"
)

type batchState struct {
	mu             sync.Mutex
	parentRows     map[string][]map[string]interface{}
	childRows      map[string]map[string][]map[string]interface{}
	connectionRows map[string]map[string]map[string]interface{}
	aggregateRows  map[string]map[string]map[string]interface{}
	cacheHits      int32
	cacheMisses    int32
}

type batchStateKey struct{}

// NewBatchingContext injects a request-scoped batch state for resolvers.
func NewBatchingContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	return context.WithValue(ctx, batchStateKey{}, &batchState{
		parentRows:     make(map[string][]map[string]interface{}),
		childRows:      make(map[string]map[string][]map[string]interface{}),
		connectionRows: make(map[string]map[string]map[string]interface{}),
		aggregateRows:  make(map[string]map[string]map[string]interface{}),
	})
}

func getBatchState(ctx context.Context) (*batchState, bool) {
	if ctx == nil {
		return nil, false
	}

	state, ok := ctx.Value(batchStateKey{}).(*batchState)
	return state, ok
}

// GetBatchState retrieves the batch state from context (exported for middleware access).
func GetBatchState(ctx context.Context) (*batchState, bool) {
	return getBatchState(ctx)
}

func (s *batchState) setParentRows(parentKey string, rows []map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.parentRows[parentKey] = rows
}

func (s *batchState) getParentRows(parentKey string) []map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.parentRows[parentKey]
}

func (s *batchState) getChildRows(relKey string) map[string][]map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.childRows[relKey]
}

func (s *batchState) setChildRows(relKey string, rows map[string][]map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.childRows[relKey] = rows
}

func (s *batchState) getAggregateRows(relKey string) map[string]map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.aggregateRows[relKey]
}

func (s *batchState) setAggregateRows(relKey string, rows map[string]map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.aggregateRows[relKey] = rows
}

func (s *batchState) getConnectionRows(relKey string) map[string]map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.connectionRows[relKey]
}

func (s *batchState) setConnectionRows(relKey string, rows map[string]map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.connectionRows[relKey] = rows
}

// IncrementCacheHit increments the cache hit counter.
func (s *batchState) IncrementCacheHit() {
	atomic.AddInt32(&s.cacheHits, 1)
}

// IncrementCacheMiss increments the cache miss counter.
func (s *batchState) IncrementCacheMiss() {
	atomic.AddInt32(&s.cacheMisses, 1)
}

// GetCacheHits returns the current cache hit count.
func (s *batchState) GetCacheHits() int32 {
	return atomic.LoadInt32(&s.cacheHits)
}

// GetCacheMisses returns the current cache miss count.
func (s *batchState) GetCacheMisses() int32 {
	return atomic.LoadInt32(&s.cacheMisses)
}
