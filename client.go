package main

import (
	"context"
	"database/sql"
	"log"
	"os/user"
	"path/filepath"
	"sync"
	"time"

	"github.com/1lann/mc-aware-remote-state/store"
	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/singleflight"
)

type fileLock struct {
	fileID  uuid.UUID
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
	Q            *store.Queries
	fileLock     map[uuid.UUID]*fileLock
	mu           sync.Mutex

	fileGroup *singleflight.Group
	pageGroup *singleflight.Group
	pagePower uint8
}

func NewSQLBackedClient(maxFiles uint64, maxPoolSize uint64, user *user.User, db *sqlx.DB, pagePower uint8) *SQLBackedClient {
	return &SQLBackedClient{
		FileMetaPool: NewFileMetaPool(maxFiles),
		PagePool:     NewPagePool(maxPoolSize, pagePower),
		User:         user,
		Q:            store.New(db),
		fileLock:     make(map[uuid.UUID]*fileLock),
		fileGroup:    &singleflight.Group{},
		pageGroup:    &singleflight.Group{},
		pagePower:    pagePower,
	}
}

var defaultTimeout = 3 * time.Second

func (c *SQLBackedClient) getFileLock(fileID uuid.UUID) *fileLock {
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
	fileID, err := uuid.FromBytes(file.FileID)
	if err != nil {
		return FileMeta{}, errors.Wrap(err, "uuid.ParseBytes")
	}

	parentID, err := uuid.ParseBytes(file.Parent)
	if err != nil {
		return FileMeta{}, errors.Wrap(err, "uuid.ParseBytes")
	}

	return FileMeta{
		FileID:     fileID,
		Parent:     parentID,
		Name:       file.Name,
		Length:     file.Length,
		FileType:   FileType(file.FileType),
		LastWrite:  time.UnixMilli(file.LastWriteAt.Int64),
		LastAccess: time.UnixMilli(file.LastAccessAt.Int64),
	}, nil
}

func (c *SQLBackedClient) GetFile(ctx context.Context, fileID uuid.UUID) (*FileMeta, error) {
	result := c.fileGroup.DoChan(fileID.String(), func() (any, error) {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()

		file, err := c.FileMetaPool.GetFile(fileID)
		if errors.Is(err, ErrNotFound) {
			// fetch from database
			fileRow, err := c.Q.GetFile(ctx, fileID[:])
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

func (c *SQLBackedClient) LookupFileInDir(ctx context.Context, dirID uuid.UUID, name string) (uuid.UUID, error) {
	next, err := c.FileMetaPool.LookupFileInDirectory(name, dirID)
	if errors.Is(err, ErrNotFound) {
		// fetch from database
		fileRow, err := c.Q.GetFileInDirectory(ctx, store.GetFileInDirectoryParams{
			Parent: dirID[:],
			Name:   name,
		})
		if errors.Is(err, sql.ErrNoRows) {
			return uuid.UUID{}, ErrNotFound
		} else if err != nil {
			return uuid.UUID{}, errors.Wrap(err, "store.GetFileInDirectory")
		}

		fileMeta, err := FileMetaFromFile(&fileRow)
		if err != nil {
			return uuid.UUID{}, err
		}

		err = c.FileMetaPool.AddFile(fileMeta, false)
		if err != nil {
			log.Println("error adding file to pool:", next)
			// skip handling error for now, the pool is merely a cache
		}
	} else if errors.Is(err, ErrTombstoned) {
		return uuid.UUID{}, ErrNotFound
	} else if err != nil {
		return uuid.UUID{}, err
	}

	return next, nil
}

func (c *SQLBackedClient) LookupFileByName(ctx context.Context, name string) (uuid.UUID, error) {
	trail := filepath.SplitList(name)

	cur := uuid.UUID{}

	for _, part := range trail {
		next, err := c.LookupFileInDir(ctx, cur, part)
		if err != nil {
			return uuid.UUID{}, err
		}

		cur = next
	}

	return cur, nil
}

func (c *SQLBackedClient) SetFileLength(ctx context.Context, fileID uuid.UUID, length int64) error {
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

	err = c.FileMetaPool.UpdateLength(fileID, length)
	if err != nil {
		return errors.Wrap(err, "FileMetaPool.UpdateLength")
	}

	return nil
}

func (c *SQLBackedClient) CreateFile(ctx context.Context, dirID uuid.UUID, name string, fileType FileType) (uuid.UUID, error) {
	parentLock := c.getFileLock(dirID)
	parentLock.RLock()
	defer func() {
		parentLock.RUnlock()
		parentLock.Release()
	}()

	// check the parent folder exists
	parentMeta, err := c.GetFile(ctx, dirID)
	if err != nil {
		return uuid.UUID{}, err
	}

	if parentMeta.FileType != FileTypeDirectory {
		return uuid.UUID{}, ErrNotSupported
	}

	// check file doesn't already exist
	_, err = c.LookupFileInDir(ctx, dirID, name)
	if err == nil {
		return uuid.UUID{}, ErrAlreadyExists
	} else if !errors.Is(err, ErrNotFound) {
		return uuid.UUID{}, err
	}

	newFileID := uuid.New()
	err = c.FileMetaPool.AddFile(FileMeta{
		FileID:     newFileID,
		Parent:     dirID,
		Name:       filepath.Base(name),
		Length:     0,
		FileType:   fileType,
		LastWrite:  time.Now().Round(time.Millisecond),
		LastAccess: time.Now().Round(time.Millisecond),
	}, true)
	if err != nil {
		return uuid.UUID{}, errors.Wrap(err, "FileMetaPool.AddFile")
	}

	return newFileID, nil
}

func (c *SQLBackedClient) DeleteFile(ctx context.Context, fileID uuid.UUID) error {
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

func (c *SQLBackedClient) DeleteDir(ctx context.Context, fileID uuid.UUID) error {
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

func (c *SQLBackedClient) RenameFile(ctx context.Context, fileID uuid.UUID, newParent uuid.UUID, newName string) error {
	log.Println("rename", fileID, newParent, newName)

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

	log.Println("rename locks acquired")

	fileMeta, err := c.GetFile(ctx, newParent)
	if err != nil {
		return err
	}

	if fileMeta.FileType != FileTypeDirectory {
		return ErrNotFound
	}

	c.FileMetaPool.ChangeName(fileID, newName)
	c.FileMetaPool.ChangeParent(fileID, newParent)

	return nil
}

func (c *SQLBackedClient) ReadDir(ctx context.Context, dirID uuid.UUID) ([]FileMeta, error) {
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

	files, err := c.Q.GetDirectoryFiles(ctx, dirID[:])
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
