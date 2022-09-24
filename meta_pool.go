package main

import (
	"container/list"
	"sync"
)

type FileMeta struct {
	Filepath string
	Length   int64
	Dirty    bool
	Element  *list.Element
}

type FileMetaPool struct {
	mu         sync.Mutex
	files      map[string]*FileMeta
	dirtyFiles []*FileMeta
	dll        *list.List
	numFiles   uint64
	maxFiles   uint64
}

func NewFileMetaPool(maxFiles uint64) *FileMetaPool {
	return &FileMetaPool{
		files:    make(map[string]*FileMeta),
		dll:      list.New(),
		maxFiles: maxFiles,
	}
}

func (p *FileMetaPool) DeleteFile(pathToFile string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	file, ok := p.files[pathToFile]
	if !ok {
		return ErrNotFound
	}

	p.deleteFile(file)

	return nil
}

func (p *FileMetaPool) deleteFile(file *FileMeta) {
	p.dll.Remove(file.Element)
	delete(p.files, file.Filepath)
	p.numFiles--
}

func (p *FileMetaPool) AddFile(pathToFile string, length int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	file, ok := p.files[pathToFile]
	if ok {
		p.deleteFile(file)
	}

	p.makeSpace()

	file = &FileMeta{
		Filepath: pathToFile,
		Length:   length,
		Dirty:    false,
	}

	p.files[pathToFile] = file
	file.Element = p.dll.PushBack(file)
	p.numFiles++
}

func (p *FileMetaPool) GetFileLength(filePath string) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	file, ok := p.files[filePath]
	if !ok {
		return 0, ErrNotFound
	}

	return file.Length, nil
}

func (p *FileMetaPool) UpdateLength(filePath string, fileLength int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	file, ok := p.files[filePath]
	if !ok {
		return ErrNotFound
	}

	if !file.Dirty {
		file.Dirty = true
		p.dirtyFiles = append(p.dirtyFiles, file)
	}
	p.dll.Remove(file.Element)
	file.Element = p.dll.PushBack(file)

	return nil
}

func (p *FileMetaPool) SwapDirtyFiles() []FileMeta {
	p.mu.Lock()
	defer p.mu.Unlock()

	files := make([]FileMeta, len(p.dirtyFiles))

	for i, page := range p.dirtyFiles {
		files[i] = *page
	}

	p.dirtyFiles = nil

	return files
}

// Evict the least recently used files to make space for bytes. Assumes lock is held.
func (p *FileMetaPool) makeSpace() {
	for p.numFiles+1 > p.maxFiles {
		front := p.dll.Front()
		file := front.Value.(*FileMeta)
		if file.Dirty {
			panic("too many dirty files")
		}
		p.deleteFile(file)
	}
}
