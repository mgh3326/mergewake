package github

import dll "container/list"

// The response cache keeps each GET's body and ETag so a repeat read is a free
// conditional request (a 304 on an authorised request does not count against
// the core rate limit). One pass touches thousands of keys, so the cache
// evicts the least recently used entry when it is full and never drops the
// rest: validators for keys the next pass reads again must survive this one.
const (
	// maxCachedEntries and maxCachedBytes bound the cache. One pass of the
	// daemon touches about a thousand keys of a few tens of KiB each.
	maxCachedEntries = 8192
	maxCachedBytes   = 128 << 20
	// maxCachedBody is the largest single body kept.
	maxCachedBody = 4 << 20
)

// responseCache is a least-recently-used cache bounded by entry count and body
// bytes. It is not safe for concurrent use: pollingState.mu guards it.
type responseCache struct {
	maxEntries, maxBytes int
	bytes                int
	order                *dll.List // front is the most recently used
	items                map[string]*dll.Element
}

type cacheItem struct {
	key  string
	resp cachedResponse
}

func newResponseCache(maxEntries, maxBytes int) *responseCache {
	return &responseCache{maxEntries: maxEntries, maxBytes: maxBytes, order: dll.New(), items: map[string]*dll.Element{}}
}

// get returns the entry for key and marks it most recently used.
func (c *responseCache) get(key string) (cachedResponse, bool) {
	e, ok := c.items[key]
	if !ok {
		return cachedResponse{}, false
	}
	c.order.MoveToFront(e)
	return e.Value.(*cacheItem).resp, true
}

// put stores resp under key and returns the keys it evicted to make room.
func (c *responseCache) put(key string, resp cachedResponse) (evicted []string) {
	if e, ok := c.items[key]; ok {
		item := e.Value.(*cacheItem)
		c.bytes += len(resp.Body) - len(item.resp.Body)
		item.resp = resp
		c.order.MoveToFront(e)
	} else {
		c.items[key] = c.order.PushFront(&cacheItem{key, resp})
		c.bytes += len(resp.Body)
	}
	for c.order.Len() > 1 && (c.order.Len() > c.maxEntries || c.bytes > c.maxBytes) {
		oldest := c.order.Back()
		item := oldest.Value.(*cacheItem)
		c.remove(oldest)
		evicted = append(evicted, item.key)
	}
	return evicted
}

func (c *responseCache) remove(e *dll.Element) {
	item := e.Value.(*cacheItem)
	c.bytes -= len(item.resp.Body)
	delete(c.items, item.key)
	c.order.Remove(e)
}

func (c *responseCache) delete(key string) {
	if e, ok := c.items[key]; ok {
		c.remove(e)
	}
}

func (c *responseCache) len() int { return c.order.Len() }

// keys lists every cached key, most recently used first.
func (c *responseCache) keys() []string {
	out := make([]string, 0, c.order.Len())
	for e := c.order.Front(); e != nil; e = e.Next() {
		out = append(out, e.Value.(*cacheItem).key)
	}
	return out
}
