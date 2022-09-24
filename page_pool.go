package main

import (
	"container/list"
	"sync"

	"github.com/cockroachdb/errors"
)

var ErrNotFound = errors.New("entry not found")
var ErrTooMuchDirt = errors.New("too many dirty entries")

type PageKey struct {
	Filepath string
	PageNum  uint32
}

type Page struct {
	Key     PageKey
	Data    []byte
	Dirty   bool
	Element *list.Element
}

type PagePool struct {
	mu         sync.Mutex
	pages      map[PageKey]*Page
	dirtyPages []*Page
	dll        *list.List
	pagePower  uint8
	size       uint64
	maxSize    uint64
}

func NewPagePool(maxSize uint64, pagePower uint8) *PagePool {
	return &PagePool{
		pages:     make(map[PageKey]*Page),
		dll:       list.New(),
		pagePower: pagePower,
		maxSize:   maxSize,
	}
}

func (p *PagePool) DeleteFile(pathToFile string, fileLength int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	maxPages := uint32(fileLength >> p.pagePower)
	for i := uint32(0); i < maxPages; i++ {
		page, ok := p.pages[PageKey{
			Filepath: pathToFile,
			PageNum:  i,
		}]
		if ok {
			p.deletePage(page)
		}
	}
}

func (p *PagePool) DeletePage(key PageKey) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	page, ok := p.pages[key]
	if !ok {
		return ErrNotFound
	}

	p.deletePage(page)
	return nil
}

func (p *PagePool) deletePage(page *Page) {
	p.dll.Remove(page.Element)
	delete(p.pages, page.Key)
	p.size -= uint64(len(page.Data))
}

func (p *PagePool) AddPage(key PageKey, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	page, ok := p.pages[key]
	if ok {
		p.deletePage(page)
	}

	if !p.makeSpace(uint64(len(data))) {
		return ErrTooMuchDirt
	}

	page = &Page{
		Key:   key,
		Data:  data,
		Dirty: false,
	}

	p.pages[key] = page
	page.Element = p.dll.PushBack(page)
	p.size += uint64(len(data))

	return nil
}

func (p *PagePool) GetPage(key PageKey) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	page, ok := p.pages[key]
	if !ok {
		return nil, ErrNotFound
	}

	return page.Data, nil
}

func (p *PagePool) WritePage(key PageKey, minLength uint64, f func(data []byte)) error {
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

		p.size += minLength - uint64(len(page.Data))

		origPage := page.Data
		page.Data = make([]byte, minLength)
		copy(page.Data, origPage)
	}

	f(page.Data)
	if !page.Dirty {
		page.Dirty = true
		p.dirtyPages = append(p.dirtyPages, page)
	}
	p.dll.Remove(page.Element)
	page.Element = p.dll.PushBack(page)

	return nil
}

func (p *PagePool) ReadPage(key PageKey) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	page, ok := p.pages[key]
	if !ok {
		return nil, ErrNotFound
	}

	return page.Data, nil
}

func (p *PagePool) SwapDirtyPages() []Page {
	p.mu.Lock()
	defer p.mu.Unlock()

	pages := make([]Page, len(p.dirtyPages))

	for i, page := range p.dirtyPages {
		pages[i] = *page
		copy(pages[i].Data, page.Data)
		page.Dirty = false
	}

	p.dirtyPages = nil

	return pages
}

// Evict the least recently used pages to make space for bytes. Assumes lock is held.
func (p *PagePool) makeSpace(bytes uint64) bool {
	for p.size+bytes > p.maxSize {
		front := p.dll.Front()
		page := front.Value.(*Page)
		if page.Dirty {
			return false
		}
		p.deletePage(page)
	}

	return true
}
