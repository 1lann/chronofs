package main

import (
	"container/list"
	"io/fs"
	"sync"

	"github.com/cockroachdb/errors"
)

var ErrNotFound = fs.ErrNotExist
var ErrTooMuchDirt = errors.New("too many dirty entries")
var ErrAlreadyExists = fs.ErrExist
var ErrNotSupported = fs.ErrInvalid
var ErrTombstoned = errors.New("tombstoned")

type PageKey struct {
	FileID  int64
	PageNum uint32
}

type Page struct {
	Key       PageKey
	Data      []byte
	tombstone bool
	dirty     bool
	pending   bool
	element   *list.Element
}

type PagePool struct {
	mu           sync.Mutex
	pages        map[PageKey]*Page
	dirtyPages   []*Page
	pendingPages []*Page
	dll          *list.List
	pagePower    uint8
	size         uint64
	maxSize      uint64
}

func NewPagePool(maxSize uint64, pagePower uint8) *PagePool {
	return &PagePool{
		pages:     make(map[PageKey]*Page),
		dll:       list.New(),
		pagePower: pagePower,
		maxSize:   maxSize,
	}
}

func (p *PagePool) TombstoneFile(fileID int64, fileLength uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	maxPages := uint32(fileLength >> p.pagePower)
	for i := uint32(0); i <= maxPages; i++ {
		page, ok := p.pages[PageKey{
			FileID:  fileID,
			PageNum: i,
		}]
		if ok {
			page.tombstone = true
			p.markDirty(page)
			p.markActivity(page)
		}
	}
}

func (p *PagePool) TombstonePage(key PageKey) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	page, ok := p.pages[key]
	if !ok {
		return ErrNotFound
	}

	page.tombstone = true
	p.markDirty(page)
	p.markActivity(page)

	return nil
}

func (p *PagePool) forgetPage(page *Page) {
	p.dll.Remove(page.element)
	delete(p.pages, page.Key)
	p.size -= uint64(len(page.Data))
}

func (p *PagePool) AddPage(key PageKey, data []byte, dirty bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	page, ok := p.pages[key]
	if !ok {
		if !p.makeSpace(uint64(len(data))) {
			return ErrTooMuchDirt
		}

		page = &Page{}
		p.pages[key] = page
	} else {
		p.size -= uint64(len(page.Data))

		if !p.makeSpace(uint64(len(data))) {
			return ErrTooMuchDirt
		}
	}

	p.size += uint64(len(data))

	page.Key = key
	page.Data = data
	page.tombstone = false
	if dirty {
		p.markDirty(page)
	}
	p.markActivity(page)

	return nil
}

func (p *PagePool) GetPage(key PageKey) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	page, ok := p.pages[key]
	if !ok {
		return nil, ErrNotFound
	}

	p.markActivity(page)

	if page.tombstone {
		return nil, ErrTombstoned
	}

	return page.Data, nil
}

func (p *PagePool) WritePage(key PageKey, minLength uint64, f func(data []byte)) error {
	if minLength > 1<<p.pagePower {
		return errors.New("minLength is too large")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	page, ok := p.pages[key]
	if !ok {
		return ErrNotFound
	}

	if uint64(len(page.Data)) < minLength {
		if !p.makeSpace(minLength - uint64(len(page.Data))) {
			return ErrTooMuchDirt
		}

		p.size += (minLength - uint64(len(page.Data)))

		origPage := page.Data
		page.Data = make([]byte, minLength)
		copy(page.Data, origPage)
	}

	f(page.Data)
	p.markDirty(page)
	p.markActivity(page)

	return nil
}

func (p *PagePool) SwapDirtyPages() []Page {
	p.mu.Lock()
	defer p.mu.Unlock()

	pages := make([]Page, len(p.dirtyPages))

	for i, page := range p.dirtyPages {
		pages[i] = *page
		copy(pages[i].Data, page.Data)
		page.dirty = false
		page.pending = true
	}

	p.pendingPages = append(p.pendingPages, p.dirtyPages...)
	p.dirtyPages = nil

	return pages
}

func (p *PagePool) CompletePending() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, page := range p.pendingPages {
		page.pending = false
	}

	p.pendingPages = nil
}

func (p *PagePool) markDirty(page *Page) {
	if !page.dirty {
		page.dirty = true
		p.dirtyPages = append(p.dirtyPages, page)
	}
}

func (p *PagePool) markActivity(page *Page) {
	if page.element != nil {
		p.dll.Remove(page.element)
	}

	page.element = p.dll.PushBack(page)
}

// Evict the least recently used pages to make space for bytes. Assumes lock is held.
func (p *PagePool) makeSpace(bytes uint64) bool {
	for p.size+bytes > p.maxSize {
		front := p.dll.Front()
		page := front.Value.(*Page)
		if page.dirty || page.pending {
			return false
		}
		p.forgetPage(page)
	}

	return true
}
