package main

import (
	"container/list"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type FileType uint8

const (
	FileTypeRegular FileType = iota
	FileTypeDirectory
)

type FileMeta struct {
	FileID     uuid.UUID
	Parent     uuid.UUID
	Name       string
	Length     int64
	FileType   FileType
	LastWrite  time.Time
	LastAccess time.Time
	dirty      bool
	tombstone  bool
	element    *list.Element
}

func (f *FileMeta) Mode() uint32 {
	if f.FileType == FileTypeDirectory {
		return 0o755 | fuse.S_IFDIR
	} else {
		return 0o644 | fuse.S_IFREG
	}
}

type FileMetaPool struct {
	mu            sync.Mutex
	files         map[uuid.UUID]*FileMeta
	filesByParent map[uuid.UUID]map[string]uuid.UUID
	dirtyFiles    []*FileMeta
	dll           *list.List
	numFiles      uint64
	maxFiles      uint64
}

func NewFileMetaPool(maxFiles uint64) *FileMetaPool {
	return &FileMetaPool{
		files:         make(map[uuid.UUID]*FileMeta),
		filesByParent: make(map[uuid.UUID]map[string]uuid.UUID),
		dll:           list.New(),
		maxFiles:      maxFiles,
	}
}

func (p *FileMetaPool) TombstoneFile(fileID uuid.UUID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	file, ok := p.files[fileID]
	if !ok {
		return ErrNotFound
	}

	file.tombstone = true
	p.markDirty(file)
	p.MarkRead(file)

	return nil
}

func (p *FileMetaPool) forgetFile(file *FileMeta) {
	p.dll.Remove(file.element)
	delete(p.files, file.FileID)
	delete(p.filesByParent, file.FileID)
	delete(p.filesByParent[file.Parent], file.Name)
	if len(p.filesByParent[file.Parent]) == 0 {
		delete(p.filesByParent, file.Parent)
	}
	p.numFiles--
}

func (p *FileMetaPool) LookupFileInDirectory(name string, dir uuid.UUID) (uuid.UUID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	files, found := p.filesByParent[dir]
	if !found {
		return uuid.UUID{}, ErrNotFound
	}

	result, found := files[name]
	if !found {
		return uuid.UUID{}, ErrNotFound
	}

	if p.files[result].tombstone {
		return uuid.UUID{}, ErrTombstoned
	}

	return result, nil
}

func (p *FileMetaPool) AddFile(meta FileMeta, dirty bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	file, ok := p.files[meta.FileID]
	if !ok {
		if !p.makeSpace() {
			return ErrTooMuchDirt
		}

		file = &FileMeta{}
		p.files[meta.FileID] = file
	} else if file.Parent != meta.Parent || file.Name != meta.Name {
		p.disassociateParent(file)
	}

	file.FileID = meta.FileID
	file.Length = meta.Length
	file.FileType = meta.FileType
	file.Parent = meta.Parent
	file.Name = meta.Name
	if dirty {
		p.markDirty(file)
	}
	file.tombstone = false

	p.associateParent(file.Parent, file.FileID)
	p.MarkRead(file)

	p.numFiles++

	return nil
}

func (p *FileMetaPool) associateParent(parentID uuid.UUID, fileID uuid.UUID) {
	parent, ok := p.filesByParent[parentID]
	if !ok {
		parent = make(map[string]uuid.UUID)
		p.filesByParent[parentID] = parent
	}
	parent[p.files[fileID].Name] = fileID
}

func (p *FileMetaPool) disassociateParent(file *FileMeta) {
	delete(p.filesByParent[file.Parent], file.Name)
	if len(p.filesByParent[file.Parent]) == 0 {
		delete(p.filesByParent, file.Parent)
	}
}

func (p *FileMetaPool) ChangeParent(fileID uuid.UUID, newParent uuid.UUID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	file, ok := p.files[fileID]
	if !ok {
		return ErrNotFound
	}

	if file.Parent != newParent {
		p.markDirty(file)
		p.disassociateParent(file)
		file.Parent = newParent
		p.associateParent(newParent, fileID)
	}

	p.MarkRead(file)

	return nil
}

func (p *FileMetaPool) ChangeName(fileID uuid.UUID, newName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	file, ok := p.files[fileID]
	if !ok {
		return ErrNotFound
	}

	if file.Name != newName {
		p.markDirty(file)
		p.disassociateParent(file)

		file.Name = newName
		p.associateParent(file.Parent, fileID)
	}

	p.MarkRead(file)

	return nil
}

func (p *FileMetaPool) GetFile(fileID uuid.UUID) (FileMeta, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if fileID == (uuid.UUID{}) {
		return FileMeta{
			FileID:     uuid.UUID{},
			Parent:     uuid.UUID{},
			Name:       "",
			Length:     0,
			FileType:   FileTypeDirectory,
			LastWrite:  time.Unix(0, 0),
			LastAccess: time.Unix(0, 0),
		}, nil
	}

	file, ok := p.files[fileID]
	if !ok {
		return FileMeta{}, ErrNotFound
	}

	p.MarkRead(file)

	if file.tombstone {
		return FileMeta{}, ErrTombstoned
	}

	return *file, nil
}

func (p *FileMetaPool) UpdateLength(fileID uuid.UUID, fileLength int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	file, ok := p.files[fileID]
	if !ok {
		return ErrNotFound
	}

	if file.Length != fileLength {
		p.markDirty(file)
		file.Length = fileLength
	}

	p.MarkRead(file)

	return nil
}

func (p *FileMetaPool) SwapDirtyFiles() []FileMeta {
	p.mu.Lock()
	defer p.mu.Unlock()

	files := make([]FileMeta, len(p.dirtyFiles))

	for i, page := range p.dirtyFiles {
		page.dirty = false
		files[i] = *page
	}

	p.dirtyFiles = nil

	return files
}

func (p *FileMetaPool) markDirty(file *FileMeta) {
	if !file.dirty {
		file.dirty = true
		p.dirtyFiles = append(p.dirtyFiles, file)
	}
	p.MarkWrite(file)
}

func (p *FileMetaPool) MarkWrite(file *FileMeta) {
	file.LastWrite = time.Now().Round(time.Millisecond)
}

func (p *FileMetaPool) MarkRead(file *FileMeta) {
	if file.element != nil {
		p.dll.Remove(file.element)
	}
	file.element = p.dll.PushBack(file)
	file.LastAccess = time.Now().Round(time.Millisecond)
}

// Evict the least recently used files to make space for bytes. Assumes lock is held.
func (p *FileMetaPool) makeSpace() bool {
	for p.numFiles+1 > p.maxFiles {
		front := p.dll.Front()
		file := front.Value.(*FileMeta)
		if file.dirty {
			return false
		}
		p.forgetFile(file)
	}

	return true
}

func (p *FileMetaPool) Union(dirID uuid.UUID, remoteFiles []FileMeta) []FileMeta {
	p.mu.Lock()
	defer p.mu.Unlock()

	pendingFiles := p.filesByParent[dirID]
	remoteFileMap := make(map[string]FileMeta)

	for _, remoteFile := range remoteFiles {
		remoteFileMap[remoteFile.Name] = remoteFile
	}

	var resultingFiles []FileMeta

	for name, file := range remoteFileMap {
		if pendingFile, ok := pendingFiles[name]; ok {
			if p.files[pendingFile].tombstone {
				continue
			}
			resultingFiles = append(resultingFiles, *p.files[pendingFile])
		} else {
			resultingFiles = append(resultingFiles, file)
		}
	}

	for name, file := range pendingFiles {
		if _, ok := remoteFileMap[name]; !ok {
			resultingFiles = append(resultingFiles, *p.files[file])
		}
	}

	sort.Slice(resultingFiles, func(i, j int) bool {
		return resultingFiles[i].Name < resultingFiles[j].Name
	})

	return resultingFiles
}
