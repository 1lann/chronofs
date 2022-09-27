package chronofs

import (
	"container/list"
	"encoding/json"
	"io/fs"
	"log"
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

type TooMuchDirtDebug struct {
	DirtySlicePages   int
	DirtySliceBytes   int
	PendingSlicePages int
	PendingSliceBytes int

	DirtyDLLPages   int
	DirtyDLLBytes   int
	PendingDLLPages int
	PendingDLLBytes int

	DirtyMapPages   int
	DirtyMapBytes   int
	PendingMapPages int
	PendingMapBytes int

	MapPages int
	MapBytes int
	DLLPages int
	DLLBytes int

	TotalSizeAccounted int
}

func (p *PagePool) debugTooMuchDirt() error {
	var debugInfo TooMuchDirtDebug

	debugInfo.TotalSizeAccounted = int(p.size)

	debugInfo.DirtySlicePages = len(p.dirtyPages)
	debugInfo.PendingSlicePages = len(p.pendingPages)

	for _, page := range p.dirtyPages {
		debugInfo.DirtySliceBytes += len(page.Data)
	}

	for _, page := range p.pendingPages {
		debugInfo.PendingSliceBytes += len(page.Data)
	}

	for e := p.dll.Front(); e != nil; e = e.Next() {
		page := e.Value.(*Page)
		if page.dirty {
			debugInfo.DirtyDLLPages++
			debugInfo.DirtyDLLBytes += len(page.Data)
		}
		if page.pending {
			debugInfo.PendingDLLPages++
			debugInfo.PendingDLLBytes += len(page.Data)
		}

		debugInfo.DLLPages++
		debugInfo.DLLBytes += len(page.Data)
	}

	for _, page := range p.pages {
		if page.dirty {
			debugInfo.DirtyMapPages++
			debugInfo.DirtyMapBytes += len(page.Data)
		}
		if page.pending {
			debugInfo.PendingMapPages++
			debugInfo.PendingMapBytes += len(page.Data)
		}

		debugInfo.MapPages++
		debugInfo.MapBytes += len(page.Data)
	}

	data, _ := json.Marshal(debugInfo)

	return errors.Wrap(ErrTooMuchDirt, string(data))
}

func (p *PagePool) forgetPage(page *Page) {
	p.dll.Remove(page.element)
	delete(p.pages, page.Key)
	p.size -= uint64(len(page.Data))
}

func (p *PagePool) AddPage(key PageKey, data []byte, dirty bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.size > (p.maxSize + 1e10) {
		log.Println("underflow has occurred in AddPage:", p.size, p.maxSize)
		p.size = 0
	}

	page, ok := p.pages[key]
	if !ok {
		if !p.makeSpace(uint64(len(data))) {
			return p.debugTooMuchDirt()
		}

		page = &Page{}
		p.pages[key] = page
	} else {
		if len(data) > len(page.Data) && !p.makeSpace(uint64(len(data)-len(page.Data))) {
			return p.debugTooMuchDirt()
		}

		p.size -= uint64(len(page.Data))
		dirty = true
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

	if p.size > (p.maxSize + 1e10) {
		log.Println("underflow has occurred in WritePage:", p.size, p.maxSize)
		p.size = 0
	}

	page, ok := p.pages[key]
	if !ok {
		return ErrNotFound
	}

	if page.pending {
		return errors.Wrap(p.debugTooMuchDirt(), "page is pending")
	}

	if uint64(len(page.Data)) < minLength {
		if !p.makeSpace(minLength - uint64(len(page.Data))) {
			return errors.Wrap(p.debugTooMuchDirt(), "not enough space")
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
	current := p.dll.Front()
	for p.size+bytes > p.maxSize && current != nil {
		next := current.Next()
		page := current.Value.(*Page)
		if page.dirty || page.pending {
			continue
		}
		p.forgetPage(page)
		current = next
	}

	if p.size+bytes > p.maxSize {
		return false
	}

	return true
}
