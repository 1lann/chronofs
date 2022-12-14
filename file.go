package chronofs

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

type FileHandle struct {
	*Node
}

type RWFileHandle interface {
	fs.FileHandle
	fs.FileReleaser
	fs.FileGetattrer
	fs.FileReader
	fs.FileWriter
	fs.FileGetlker
	fs.FileSetlker
	fs.FileSetlkwer
	fs.FileLseeker
	fs.FileFlusher
	fs.FileFsyncer
	fs.FileSetattrer
	fs.FileAllocater
}

var _ = (RWFileHandle)((*FileHandle)(nil))

func (f *FileHandle) Release(ctx context.Context) syscall.Errno {
	return f.Fsync(ctx, 0)
}

func (f *FileHandle) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	return f.Node.Getattr(ctx, f, out)
}

func (f *FileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	bytesRead, err := f.Node.context.ReadFile(context.Background(), f.Node.fileID, off, dest)
	if err != nil {
		return nil, errToSyscall(err)
	}

	return fuse.ReadResultData(dest[:bytesRead]), 0

}

func (f *FileHandle) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	err := f.Node.context.WriteFile(context.Background(), f.Node.fileID, off, data, f.context.fsyncTimeout)
	if err != nil {
		return 0, errToSyscall(err)
	}

	return uint32(len(data)), 0
}

func (f *FileHandle) Getlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) syscall.Errno {
	return syscall.ENOTSUP
}

func (f *FileHandle) Setlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	return syscall.ENOTSUP
}

// See NodeReleaser.
type FileReleaser interface {
	Release(ctx context.Context) syscall.Errno
	Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno
	Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno)
	Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno)
	Getlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) syscall.Errno
	Setlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno
	Setlkw(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno
	Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno)
	Flush(ctx context.Context) syscall.Errno
	Fsync(ctx context.Context, flags uint32) syscall.Errno
	Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno
	Allocate(ctx context.Context, off uint64, size uint64, mode uint32) syscall.Errno
}

func (f *FileHandle) Setlkw(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	return syscall.ENOTSUP
}

func (f *FileHandle) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	fileMeta, err := f.context.GetFile(ctx, f.Node.fileID)
	if err != nil {
		return 0, errToSyscall(err)
	}

	switch whence {
	case unix.SEEK_SET:
		break
	case unix.SEEK_CUR:
		panic("unsupported SEEK_CUR")
	case unix.SEEK_END:
		off += uint64(fileMeta.Length)
	case unix.SEEK_HOLE:
		off = uint64(fileMeta.Length)
	case unix.SEEK_DATA:
		break
	}

	return off, 0
}

func (f *FileHandle) Flush(ctx context.Context) syscall.Errno {
	return 0
}

func (f *FileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return errToSyscall(f.Node.context.Fsync(context.Background(), f.Node.fileID))
}

func (f *FileHandle) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return f.Node.Setattr(ctx, f, in, out)
}

func (f *FileHandle) Allocate(ctx context.Context, off uint64, size uint64, mode uint32) syscall.Errno {
	fileMeta, err := f.context.GetFile(ctx, f.Node.fileID)
	if err != nil {
		return errToSyscall(err)
	}

	if off+size > uint64(fileMeta.Length) {
		err := f.context.SetFileLength(ctx, f.Node.fileID, int64(off+size))
		if err != nil {
			return errToSyscall(err)
		}
	}

	return 0
}
