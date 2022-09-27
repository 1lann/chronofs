package chronofs

import (
	"container/list"
	"log"
	"os/user"
	"sort"
	"sync"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type FileType uint8

const (
	FileTypeRegular FileType = iota
	FileTypeDirectory
	FileTypeSymlink
)

type FileMeta struct {
	FileID      int64
	Parent      int64
	Name        string
	Length      int64
	FileType    FileType
	Link        string
	Permissions int64
	Owner       int64
	Group       int64
	LastWrite   time.Time
	LastAccess  time.Time
	dirty       bool
	pending     bool
	tombstone   bool
	element     *list.Element
}

func (f *FileMeta) Mode() uint32 {
	perms := uint32(f.Permissions & 0o777)
	switch f.FileType {
	case FileTypeRegular:
		return fuse.S_IFREG | perms
	case FileTypeDirectory:
		return fuse.S_IFDIR | perms
	case FileTypeSymlink:
		return fuse.S_IFLNK | perms
	default:
		log.Println("unknown file type??")
		return fuse.S_IFREG | perms
	}
}

type FileMetaPool struct {
	mu            sync.Mutex
	files         map[int64]*FileMeta
	filesByParent map[int64]map[string]int64
	dirtyFiles    []*FileMeta
	pendingFiles  []*FileMeta
	dll           *list.List
	numFiles      uint64
	maxFiles      uint64
	defaultUser   *user.User
}

func NewFileMetaPool(maxFiles uint64, defaultUser *user.User) *FileMetaPool {
	return &FileMetaPool{
		files:         make(map[int64]*FileMeta),
		filesByParent: make(map[int64]map[string]int64),
		dll:           list.New(),
		maxFiles:      maxFiles,
		defaultUser:   defaultUser,
	}
}

func (p *FileMetaPool) TombstoneFile(fileID int64) error {
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

	if len(p.filesByParent[file.FileID]) == 0 {
		delete(p.filesByParent, file.FileID)
	}

	delete(p.filesByParent[file.Parent], file.Name)
	if len(p.filesByParent[file.Parent]) == 0 {
		delete(p.filesByParent, file.Parent)
	}
	p.numFiles--
}

func (p *FileMetaPool) LookupFileInDirectory(name string, dir int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	files, found := p.filesByParent[dir]
	if !found {
		return 0, ErrNotFound
	}

	result, found := files[name]
	if !found {
		return 0, ErrNotFound
	}

	if p.files[result].tombstone {
		return 0, ErrTombstoned
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
	file.Link = meta.Link
	file.Permissions = meta.Permissions
	file.Owner = meta.Owner
	file.Group = meta.Group
	file.LastWrite = meta.LastWrite
	file.LastAccess = meta.LastAccess
	if dirty {
		p.markDirty(file)
	}
	file.tombstone = false

	p.associateParent(file.Parent, file.FileID)
	p.MarkRead(file)

	p.numFiles++

	return nil
}

func (p *FileMetaPool) associateParent(parentID int64, fileID int64) {
	parent, ok := p.filesByParent[parentID]
	if !ok {
		parent = make(map[string]int64)
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

func (p *FileMetaPool) ChangeParent(fileID int64, newParent int64) error {
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

func (p *FileMetaPool) ChangeName(fileID int64, newName string) error {
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

func (p *FileMetaPool) GetFile(fileID int64) (FileMeta, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if fileID >= 0 && fileID <= 2 {
		uid, gid, err := getUserGroupID(p.defaultUser)
		if err != nil {
			return FileMeta{}, err
		}

		return FileMeta{
			FileID:      2,
			Parent:      0,
			Name:        "",
			Length:      0,
			FileType:    FileTypeDirectory,
			Link:        "",
			Permissions: 0o755,
			Owner:       uid,
			Group:       gid,
			LastWrite:   time.Unix(0, 0),
			LastAccess:  time.Unix(0, 0),
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

func (p *FileMetaPool) UpdateFileAttr(fileID int64, f func(*FileMeta)) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	file, ok := p.files[fileID]
	if !ok {
		return ErrNotFound
	}

	f(file)
	p.markDirty(file)
	p.MarkRead(file)

	return nil
}

func (p *FileMetaPool) SwapDirtyFiles() []FileMeta {
	p.mu.Lock()
	defer p.mu.Unlock()

	files := make([]FileMeta, len(p.dirtyFiles))

	for i, page := range p.dirtyFiles {
		page.dirty = false
		page.pending = true
		files[i] = *page
	}

	p.pendingFiles = append(p.pendingFiles, p.dirtyFiles...)
	p.dirtyFiles = nil

	return files
}

func (p *FileMetaPool) FailPending() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, file := range p.pendingFiles {
		file.pending = false

		if !file.dirty {
			file.dirty = true
			p.dirtyFiles = append(p.dirtyFiles, file)
		}
	}

	p.pendingFiles = nil
}

func (p *FileMetaPool) CompletePending() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, file := range p.pendingFiles {
		file.pending = false
	}

	p.pendingFiles = nil
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
	current := p.dll.Front()
	for p.numFiles+1 > p.maxFiles && current != nil {
		next := current.Next()
		file := current.Value.(*FileMeta)
		if file.dirty || file.pending {
			current = next
			continue
		}
		p.forgetFile(file)
		current = next
	}

	if p.numFiles+1 > p.maxFiles {
		return false
	}

	return true
}

func (p *FileMetaPool) Union(dirID int64, remoteFiles []FileMeta) []FileMeta {
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
			if p.files[file].tombstone {
				continue
			}
			resultingFiles = append(resultingFiles, *p.files[file])
		}
	}

	sort.Slice(resultingFiles, func(i, j int) bool {
		return resultingFiles[i].Name < resultingFiles[j].Name
	})

	return resultingFiles
}
