package chronofs

import (
	"context"
	"io"
	stdfs "io/fs"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

type StdFileInfo struct {
	*FileMeta
}

func (f StdFileInfo) Name() string {
	return f.FileMeta.Name
}

func (f StdFileInfo) Size() int64 {
	return f.FileMeta.Length
}

func (f StdFileInfo) Mode() stdfs.FileMode {
	return stdfs.FileMode(f.FileMeta.Mode())
}

func (f StdFileInfo) Type() stdfs.FileMode {
	return f.Mode().Type()
}

func (f StdFileInfo) ModTime() time.Time {
	return f.FileMeta.LastWrite
}

func (f StdFileInfo) IsDir() bool {
	return f.FileType == FileTypeDirectory
}

func (f StdFileInfo) Info() (stdfs.FileInfo, error) {
	return f, nil
}

func (f StdFileInfo) Sys() any {
	return f.FileMeta
}

var _ = (stdfs.FileInfo)(StdFileInfo{})
var _ = (stdfs.DirEntry)(StdFileInfo{})

type CompleteStdFS interface {
	stdfs.FS
	stdfs.ReadDirFS
	stdfs.StatFS
	stdfs.SubFS
}

type StdFS struct {
	client FSClient
	fileID int64
}

func NewRootStdFS(client FSClient) *StdFS {
	return NewStdFS(client, RootID)
}

func NewStdFS(client FSClient, fileID int64) *StdFS {
	return &StdFS{
		client: client,
		fileID: fileID,
	}
}

var _ = (CompleteStdFS)((*StdFS)(nil))

type StdFile struct {
	client FSClient
	fileID int64
	offset int64
	closed atomic.Bool
}

var _ = (stdfs.File)((*StdFile)(nil))
var _ = (io.ReaderAt)((*StdFile)(nil))
var _ = (io.Seeker)((*StdFile)(nil))
var _ = (stdfs.ReadDirFile)((*StdFile)(nil))

func standardizePath(name string) string {
	name = strings.TrimPrefix(name, "./")

	return filepath.Clean(strings.TrimRight(strings.TrimLeft(name, "/"), "/"))
}

func (f *StdFile) ReadDir(n int) ([]stdfs.DirEntry, error) {
	ctx := context.Background()

	fileMetas, err := f.client.ReadDir(ctx, f.fileID)
	if err != nil {
		return nil, err
	}

	dirEntries := make([]stdfs.DirEntry, len(fileMetas))
	for i, fileMeta := range fileMetas {
		fileMetaCopy := fileMeta
		dirEntries[i] = StdFileInfo{&fileMetaCopy}
	}

	if n > 0 && n < len(dirEntries) {
		return dirEntries[:n], nil
	}

	return dirEntries, nil
}

func (f *StdFile) ReadAt(p []byte, off int64) (int, error) {
	if f.closed.Load() {
		return 0, stdfs.ErrClosed
	}

	return f.client.ReadFile(context.Background(), f.fileID, off, p)
}

func (f *StdFile) Seek(offset int64, whence int) (int64, error) {
	if f.closed.Load() {
		return 0, stdfs.ErrClosed
	}

	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		info, err := f.Stat()
		if err != nil {
			return 0, err
		}

		f.offset = info.Size() + offset
	default:
		return 0, stdfs.ErrInvalid
	}

	return f.offset, nil
}

func (f *StdFile) WriteTo(w io.Writer) (int64, error) {
	if f.closed.Load() {
		return 0, stdfs.ErrClosed
	}

	return f.client.DumpFileNoCache(context.Background(), f.fileID, w)
}

func (f *StdFile) Read(p []byte) (n int, err error) {
	if f.closed.Load() {
		return 0, stdfs.ErrClosed
	}

	bytesRead, err := f.client.ReadFile(context.Background(), f.fileID, f.offset, p)
	if err != nil {
		return 0, err
	}

	f.offset += int64(bytesRead)

	if bytesRead == 0 {
		return 0, io.EOF
	}

	return bytesRead, nil
}

func (f *StdFile) Close() error {
	f.closed.Store(true)
	return nil
}

func (f *StdFile) Stat() (stdfs.FileInfo, error) {
	fileMeta, err := f.client.GetFile(context.Background(), f.fileID)
	if err != nil {
		return nil, err
	}

	return StdFileInfo{fileMeta}, nil
}

func (s *StdFS) Open(name string) (stdfs.File, error) {
	ctx := context.Background()

	fileID, err := s.standardizeLookupByPath(ctx, name)
	if err != nil {
		return nil, err
	}

	return &StdFile{
		client: s.client,
		fileID: fileID,
	}, nil
}

func (s *StdFS) standardizeLookupByPath(ctx context.Context, name string) (int64, error) {
	name = standardizePath(name)

	if name == "." {
		return s.fileID, nil
	}

	return s.client.LookupFileByPath(ctx, s.fileID, name)
}

func (s *StdFS) ReadDir(name string) ([]stdfs.DirEntry, error) {
	ctx := context.Background()

	fileID, err := s.standardizeLookupByPath(ctx, name)
	if err != nil {
		return nil, err
	}

	fileMetas, err := s.client.ReadDir(ctx, fileID)
	if err != nil {
		return nil, err
	}

	dirEntries := make([]stdfs.DirEntry, len(fileMetas))
	for i, fileMeta := range fileMetas {
		fileMetaCopy := fileMeta
		dirEntries[i] = StdFileInfo{&fileMetaCopy}
	}

	return dirEntries, nil
}

func (s *StdFS) Stat(name string) (stdfs.FileInfo, error) {
	ctx := context.Background()

	fileID, err := s.standardizeLookupByPath(ctx, name)
	if err != nil {
		return nil, err
	}

	fileMeta, err := s.client.GetFile(ctx, fileID)
	if err != nil {
		return nil, err
	}

	return StdFileInfo{fileMeta}, nil
}

func (s *StdFS) Sub(dir string) (stdfs.FS, error) {
	ctx := context.Background()

	fileID, err := s.standardizeLookupByPath(ctx, dir)
	if err != nil {
		return nil, err
	}

	fileMeta, err := s.client.GetFile(ctx, fileID)
	if err != nil {
		return nil, err
	}

	if fileMeta.FileType != FileTypeDirectory {
		return nil, stdfs.ErrInvalid
	}

	return &StdFS{
		client: s.client,
		fileID: fileID,
	}, nil
}
