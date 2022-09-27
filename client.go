package chronofs

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"log"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/1lann/chronofs/store"
	"github.com/cockroachdb/errors"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/singleflight"
)

const RootID = 2

type FSClient interface {
	GetFile(ctx context.Context, fileID int64) (*FileMeta, error)
	LookupFileInDir(ctx context.Context, dirID int64, name string) (int64, error)
	LookupFileByPath(ctx context.Context, dirID int64, name string) (int64, error)
	SetFileAttrs(ctx context.Context, fileID int64, f func(*FileMeta)) error
	SetFileLength(ctx context.Context, fileID int64, length int64) error
	CreateFile(ctx context.Context, dirID int64, name string, perms int64, fileType FileType,
		link string, uid int64, gid int64) (int64, error)
	DeleteFile(ctx context.Context, fileID int64) error
	DeleteDir(ctx context.Context, fileID int64) error
	RenameFile(ctx context.Context, fileID int64, newParent int64, newName string) error
	ReadDir(ctx context.Context, dirID int64) ([]FileMeta, error)
	Sync(ctx context.Context) error
	ReadFile(ctx context.Context, fileID int64, offset int64, data []byte) (int, error)
	WriteFile(ctx context.Context, fileID int64, offset int64, data []byte) error
	DumpFileNoCache(ctx context.Context, fileID int64, wr io.Writer) (int64, error)
}

type fileLock struct {
	fileID  int64
	mu      sync.RWMutex
	clients int64
	c       *SQLBackedClient
}

func (l *fileLock) RLock() {
	l.mu.RLock()
}

func (l *fileLock) RUnlock() {
	l.mu.RUnlock()
}

func (l *fileLock) Lock() {
	l.mu.Lock()
}

func (l *fileLock) Unlock() {
	l.mu.Unlock()
}

func (l *fileLock) Release() {
	l.c.mu.Lock()
	defer l.c.mu.Unlock()

	l.clients--

	if l.clients == 0 {
		delete(l.c.fileLock, l.fileID)
	}
}

type SQLBackedClient struct {
	FileMetaPool *FileMetaPool
	PagePool     *PagePool
	User         *user.User
	DB           *sqlx.DB
	Q            *store.Queries
	fileLock     map[int64]*fileLock
	mu           sync.Mutex

	fileGroup *singleflight.Group
	pageGroup *singleflight.Group
	pagePower uint8
}

func NewSQLBackedClient(maxFiles uint64, maxPoolSize uint64, defaultUser *user.User,
	db *sqlx.DB, pagePower uint8) *SQLBackedClient {
	return &SQLBackedClient{
		FileMetaPool: NewFileMetaPool(maxFiles, defaultUser),
		PagePool:     NewPagePool(maxPoolSize, pagePower),
		User:         defaultUser,
		DB:           db,
		Q:            store.New(db),
		fileLock:     make(map[int64]*fileLock),
		fileGroup:    &singleflight.Group{},
		pageGroup:    &singleflight.Group{},
		pagePower:    pagePower,
	}
}

var defaultTimeout = 3 * time.Second

func (c *SQLBackedClient) getFileLock(fileID int64) *fileLock {
	c.mu.Lock()
	defer c.mu.Unlock()

	lock, ok := c.fileLock[fileID]
	if !ok {
		lock = &fileLock{
			fileID: fileID,
			c:      c,
		}
		c.fileLock[fileID] = lock
	}

	lock.clients++

	return lock
}

func FileMetaFromFile(file *store.File) (FileMeta, error) {
	return FileMeta{
		FileID:      file.FileID,
		Parent:      file.Parent,
		Name:        file.Name,
		Length:      file.Length,
		Link:        file.Link,
		Permissions: file.Permissions,
		Owner:       file.OwnerID,
		Group:       file.GroupID,
		FileType:    FileType(file.FileType),
		LastWrite:   time.UnixMilli(file.LastWriteAt),
		LastAccess:  time.UnixMilli(file.LastAccessAt),
	}, nil
}

func (c *SQLBackedClient) GetFile(ctx context.Context, fileID int64) (*FileMeta, error) {
	result := c.fileGroup.DoChan(strconv.FormatInt(fileID, 10), func() (any, error) {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()

		file, err := c.FileMetaPool.GetFile(fileID)
		if errors.Is(err, ErrNotFound) {
			// fetch from database
			fileRow, err := c.Q.GetFile(ctx, fileID)
			if errors.Is(err, sql.ErrNoRows) {
				return nil, ErrNotFound
			} else if err != nil {
				return nil, errors.Wrap(err, "store.GetFile")
			}

			fileMeta, err := FileMetaFromFile(&fileRow)
			if err != nil {
				return nil, err
			}

			err = c.FileMetaPool.AddFile(fileMeta, false)
			if err != nil {
				log.Println("error adding file to pool:", fileID)
				// skip handling error for now, the pool is merely a cache
			}

			return &fileMeta, nil
		} else if errors.Is(err, ErrTombstoned) {
			return nil, ErrNotFound
		} else if err != nil {
			return nil, errors.Wrap(err, "FileMetaPool.GetFile")
		}

		return &file, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-result:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*FileMeta), nil
	}
}

func (c *SQLBackedClient) LookupFileInDir(ctx context.Context, dirID int64, name string) (int64, error) {
	next, err := c.FileMetaPool.LookupFileInDirectory(name, dirID)
	if errors.Is(err, ErrNotFound) {
		// fetch from database
		fileRow, err := c.Q.GetFileInDirectory(ctx, store.GetFileInDirectoryParams{
			Parent: dirID,
			Name:   name,
		})
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrNotFound
		} else if err != nil {
			return 0, errors.Wrap(err, "store.GetFileInDirectory")
		}

		fileMeta, err := FileMetaFromFile(&fileRow)
		if err != nil {
			return 0, err
		}

		err = c.FileMetaPool.AddFile(fileMeta, false)
		if err != nil {
			log.Println("error adding file to pool:", next)
			// skip handling error for now, the pool is merely a cache
		}

		next = fileMeta.FileID
	} else if errors.Is(err, ErrTombstoned) {
		return 0, ErrNotFound
	} else if err != nil {
		return 0, err
	}

	return next, nil
}

func (c *SQLBackedClient) LookupFileByPath(ctx context.Context, dirID int64, name string) (int64, error) {
	trail := strings.Split(name, "/")

	cur := dirID

	for _, part := range trail {
		next, err := c.LookupFileInDir(ctx, cur, part)
		if err != nil {
			return 0, err
		}

		cur = next
	}

	return cur, nil
}

func (c *SQLBackedClient) SetFileAttrs(ctx context.Context, fileID int64, f func(*FileMeta)) error {
	lock := c.getFileLock(fileID)
	lock.Lock()
	defer func() {
		lock.Unlock()
		lock.Release()
	}()

	// call GetFile to ensure that the file is in the pool
	_, err := c.GetFile(ctx, fileID)
	if err != nil {
		return errors.Wrap(err, "GetFile")
	}

	return c.FileMetaPool.UpdateFileAttr(fileID, func(file *FileMeta) {
		fileCopy := *file

		f(&fileCopy)

		file.Link = fileCopy.Link
		file.Permissions = fileCopy.Permissions
		file.Owner = fileCopy.Owner
		file.Group = fileCopy.Group
		file.LastAccess = fileCopy.LastAccess
		file.LastWrite = fileCopy.LastWrite
	})
}

func (c *SQLBackedClient) SetFileLength(ctx context.Context, fileID int64, length int64) error {
	if length < 0 {
		return errors.New("length must be positive")
	}

	lock := c.getFileLock(fileID)
	lock.Lock()
	defer func() {
		lock.Unlock()
		lock.Release()
	}()

	fileMeta, err := c.GetFile(ctx, fileID)
	if err != nil {
		return err
	}

	if fileMeta.FileType != FileTypeRegular {
		return ErrNotSupported
	}

	if length == fileMeta.Length {
		return nil
	}

	if length < fileMeta.Length {
		// zero pages
		err = c.writeFileNoLock(ctx, fileMeta, length, make([]byte, fileMeta.Length-length))
		if err != nil {
			return err
		}
	}

	err = c.FileMetaPool.UpdateFileAttr(fileID, func(f *FileMeta) {
		f.Length = length
	})
	if err != nil {
		return errors.Wrap(err, "FileMetaPool.UpdateFileAttr")
	}

	return nil
}

// DumpFileNoCache provides an efficient way to extract an entire file from the database without
// any page caching into a writer. This is used by `extract`. Note that this method will still
// use the file metadata cache.
func (c *SQLBackedClient) DumpFileNoCache(ctx context.Context, fileID int64, wr io.Writer) (int64, error) {
	fileMeta, err := c.GetFile(ctx, fileID)
	if err != nil {
		return 0, err
	}

	maxPage := (fileMeta.Length - 1) >> int64(c.pagePower)

	var bytesWritten int64

	for pageNum := int64(0); pageNum <= maxPage; pageNum++ {
		pageSize := int64(1) << c.pagePower
		if pageNum == maxPage {
			pageSize = int64(fileMeta.Length) - (int64(maxPage) << int64(c.pagePower))
		}

		pageData, err := c.Q.GetPage(ctx, store.GetPageParams{
			FileID:        fileID,
			PageNum:       pageNum,
			PageSizePower: int64(c.pagePower),
		})
		if err != nil {
			return bytesWritten, errors.Wrapf(err, "fetch page %d", pageNum)
		}

		pageLimit := pageSize
		if int64(len(pageData)) < pageLimit {
			pageLimit = int64(len(pageData))
		}

		bytesCopied, err := io.Copy(wr, bytes.NewReader(pageData[:pageLimit]))
		bytesWritten += bytesCopied
		if err != nil {
			return bytesWritten, errors.Wrapf(err, "copy page %d", pageNum)
		}

		if bytesCopied < int64(pageSize) {
			// zero pad
			bytesCopied, err := io.Copy(wr, bytes.NewReader(make([]byte, pageSize-bytesCopied)))
			bytesWritten += bytesCopied
			if err != nil {
				return bytesWritten, errors.Wrap(err, "copy zero pad")
			}
		}
	}

	return bytesWritten, nil
}

func (c *SQLBackedClient) CreateFile(ctx context.Context, dirID int64, name string, perms int64,
	fileType FileType, link string, uid int64, gid int64) (int64, error) {
	parentLock := c.getFileLock(dirID)
	parentLock.RLock()
	defer func() {
		parentLock.RUnlock()
		parentLock.Release()
	}()

	// check the parent folder exists
	parentMeta, err := c.GetFile(ctx, dirID)
	if err != nil {
		return 0, err
	}

	if parentMeta.FileType != FileTypeDirectory {
		return 0, ErrNotSupported
	}

	// check file doesn't already exist
	_, err = c.LookupFileInDir(ctx, dirID, name)
	if err == nil {
		return 0, ErrAlreadyExists
	} else if !errors.Is(err, ErrNotFound) {
		return 0, err
	}

	now := time.Now().Round(time.Millisecond)

	// create file
	fileID, err := c.Q.CreateFile(ctx, store.CreateFileParams{
		Parent:       dirID,
		Name:         name,
		FileType:     int64(fileType),
		Length:       0,
		Link:         link,
		Permissions:  perms,
		OwnerID:      uid,
		GroupID:      gid,
		LastWriteAt:  now.UnixMilli(),
		LastAccessAt: now.UnixMilli(),
	})
	if err != nil {
		return 0, errors.Wrap(err, "store.CreateFile")
	}

	err = c.FileMetaPool.AddFile(FileMeta{
		FileID:      fileID,
		Parent:      dirID,
		Name:        name,
		FileType:    fileType,
		Length:      0,
		Link:        link,
		Permissions: perms,
		Owner:       uid,
		Group:       gid,
		LastWrite:   now,
		LastAccess:  now,
	}, true)
	if err != nil {
		return 0, errors.Wrap(err, "FileMetaPool.AddFile")
	}

	return fileID, nil
}

func (c *SQLBackedClient) DeleteFile(ctx context.Context, fileID int64) error {
	lock := c.getFileLock(fileID)
	lock.Lock()
	defer func() {
		lock.Unlock()
		lock.Release()
	}()

	fileMeta, err := c.GetFile(ctx, fileID)
	if err != nil {
		return err
	}

	if fileMeta.FileType == FileTypeDirectory {
		return ErrNotSupported
	}

	c.PagePool.TombstoneFile(fileID, uint64(fileMeta.Length))
	c.FileMetaPool.TombstoneFile(fileID)

	return nil
}

func (c *SQLBackedClient) DeleteDir(ctx context.Context, fileID int64) error {
	lock := c.getFileLock(fileID)
	lock.Lock()
	defer func() {
		lock.Unlock()
		lock.Release()
	}()

	fileMeta, err := c.GetFile(ctx, fileID)
	if err != nil {
		return err
	}

	if fileMeta.FileType != FileTypeDirectory {
		return ErrNotSupported
	}

	c.FileMetaPool.TombstoneFile(fileID)

	return nil
}

func (c *SQLBackedClient) RenameFile(ctx context.Context, fileID int64, newParent int64, newName string) error {
	dirLock := c.getFileLock(newParent)
	dirLock.RLock()
	defer func() {
		dirLock.RUnlock()
		dirLock.Release()
	}()

	lock := c.getFileLock(fileID)
	lock.Lock()
	defer func() {
		lock.Unlock()
		lock.Release()
	}()

	fileMeta, err := c.GetFile(ctx, newParent)
	if err != nil {
		return err
	}

	if fileMeta.FileType != FileTypeDirectory {
		return ErrNotFound
	}

	c.Q.RenameFile(ctx, store.RenameFileParams{
		FileID: fileID,
		Name:   newName,
		Parent: newParent,
	})

	c.FileMetaPool.ChangeName(fileID, newName)
	c.FileMetaPool.ChangeParent(fileID, newParent)

	return nil
}

func (c *SQLBackedClient) ReadDir(ctx context.Context, dirID int64) ([]FileMeta, error) {
	lock := c.getFileLock(dirID)
	lock.RLock()
	defer func() {
		lock.RUnlock()
		lock.Release()
	}()

	fileMeta, err := c.GetFile(ctx, dirID)
	if err != nil {
		return nil, err
	}

	if fileMeta.FileType != FileTypeDirectory {
		return nil, ErrNotSupported
	}

	files, err := c.Q.GetDirectoryFiles(ctx, dirID)
	if err != nil {
		return nil, errors.Wrap(err, "GetDirectoryFiles")
	}

	remoteFiles := make([]FileMeta, 0, len(files))

	for _, file := range files {
		fileMeta, err := FileMetaFromFile(&file)
		if err != nil {
			return nil, errors.Wrap(err, "FileMetaFromFile")
		}
		remoteFiles = append(remoteFiles, fileMeta)
	}

	return c.FileMetaPool.Union(dirID, remoteFiles), nil
}

// syncs the state of the filesystem to disk.
func (c *SQLBackedClient) Sync(ctx context.Context, final ...bool) error {
	files := c.FileMetaPool.SwapDirtyFiles()
	pages := c.PagePool.SwapDirtyPages()

	log.Printf("sync has %d files and %d pages", len(files), len(pages))

	if len(files) == 0 && len(pages) == 0 {
		return nil
	}

	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "BeginTx")
	}

	err = func() error {
		q := c.Q.WithTx(tx)

		for _, file := range files {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if file.tombstone {
				_, err := q.DeleteFile(ctx, file.FileID)
				if err != nil {
					return errors.Wrap(err, "DeleteFile")
				}
			} else {
				err := q.UpdateFile(ctx, store.UpdateFileParams{
					FileID:       file.FileID,
					Parent:       file.Parent,
					Name:         file.Name,
					FileType:     int64(file.FileType),
					Length:       file.Length,
					Link:         file.Link,
					Permissions:  file.Permissions,
					OwnerID:      file.Owner,
					GroupID:      file.Group,
					LastWriteAt:  file.LastWrite.UnixMilli(),
					LastAccessAt: file.LastAccess.UnixMilli(),
				})
				if err != nil {
					return errors.Wrap(err, "UpdateFile")
				}
			}
		}

		for _, page := range pages {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if page.tombstone {
				_, err := q.DeletePage(ctx, store.DeletePageParams{
					FileID:        page.Key.FileID,
					PageNum:       int64(page.Key.PageNum),
					PageSizePower: int64(c.pagePower),
				})
				if err != nil {
					return errors.Wrap(err, "DeletePage")
				}
			} else {
				err := q.UpsertPage(ctx, store.UpsertPageParams{
					FileID:        page.Key.FileID,
					PageNum:       int64(page.Key.PageNum),
					PageSizePower: int64(c.pagePower),
					Data:          page.Data,
				})
				if err != nil {
					return errors.Wrap(err, "UpsertPage")
				}
			}
		}

		return nil
	}()
	if err != nil {
		tx.Rollback()
		return errors.Wrap(err, "tx execution")
	}

	err = tx.Commit()
	if err != nil {
		return errors.Wrap(err, "Commit")
	}

	if len(final) == 0 || !final[0] {
		// complete pending only when it's not the final sync
		c.FileMetaPool.CompletePending()
		c.PagePool.CompletePending()
	}

	return nil
}
