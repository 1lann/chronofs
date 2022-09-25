package main

import (
	"context"
	"log"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type Node struct {
	fs.Inode
	fileID   int64
	fileType FileType
	client   *SQLBackedClient
}

type RWNode interface {
	fs.NodeGetattrer
	fs.NodeSetattrer
	// fs.NodeGetxattrer
	// fs.NodeSetxattrer
	// fs.NodeRemovexattrer
	// fs.NodeListxattrer
	fs.NodeReadlinker
	fs.NodeOpener
	// fs.NodeCopyFileRanger
	fs.NodeLookuper
	fs.NodeOpendirer
	fs.NodeReaddirer
	fs.NodeMkdirer
	fs.NodeMknoder
	fs.NodeLinker
	fs.NodeSymlinker
	fs.NodeUnlinker
	fs.NodeRmdirer
	fs.NodeRenamer
}

var _ = (RWNode)((*Node)(nil))

func (n *Node) Access(ctx context.Context, mask uint32) syscall.Errno {
	return 0
}

func (n *Node) getAttrResponse(file *FileMeta, out *fuse.Attr) error {
	out.Nlink = 1
	out.Mode = file.Mode()
	out.Size = uint64(file.Length)

	if file.FileType == FileTypeDirectory {
		out.Size = 4096
	}

	out.Blocks = uint64(file.Length >> 9)
	out.Atime = uint64(file.LastAccess.Unix())
	out.Mtime = uint64(file.LastWrite.Unix())
	out.Atimensec = uint32(file.LastAccess.Nanosecond())
	out.Mtimensec = uint32(file.LastWrite.Nanosecond())
	uid, err := strconv.Atoi(n.client.User.Uid)
	if err != nil {
		log.Println("failed to parse uid", err)
		return syscall.EIO
	}

	gid, err := strconv.Atoi(n.client.User.Gid)
	if err != nil {
		log.Println("failed to parse gid", err)
		return syscall.EIO
	}

	out.Owner = fuse.Owner{
		Uid: uint32(uid),
		Gid: uint32(gid),
	}

	out.Blksize = 4096
	out.Padding = 0

	return nil
}

func (n *Node) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	file, err := n.client.GetFile(ctx, n.fileID)
	if err != nil {
		return errToSyscall(err)
	}

	out.SetTimeout(time.Second)
	if err := n.getAttrResponse(file, &out.Attr); err != nil {
		return errToSyscall(err)
	}

	if fh != nil {
		fileHandle := fh.(*FileHandle)
		log.Println("oh actually i got a file handle:", fileHandle.fileID)
	}

	return 0
}

func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if size, ok := in.GetSize(); ok {
		log.Println("setting filesize to", size)
		err := n.client.SetFileLength(ctx, n.fileID, int64(size))
		if err != nil {
			return errToSyscall(err)
		}
	}

	return n.Getattr(ctx, f, out)
}

func (n *Node) OnAdd(ctx context.Context) {}

// 	Readlink(ctx context.Context) ([]byte, syscall.Errno)

func (n *Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	return nil, syscall.ENOENT
}

func standardizeName(name string) string {
	return filepath.Clean("/" + name)
}

func errToSyscall(err error) syscall.Errno {
	if errors.Is(err, ErrNotFound) {
		return syscall.ENOENT
	} else if errors.Is(err, ErrNotSupported) {
		return syscall.ENOTSUP
	} else if errors.Is(err, ErrAlreadyExists) {
		return syscall.EEXIST
	} else if errors.Is(err, ErrTombstoned) {
		// shouldn't really happen??
		return syscall.ENOENT
	} else if err != nil {
		log.Printf("unknown error: %+v", err)
		return syscall.EIO
	}

	return 0
}

func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n.fileType != FileTypeDirectory {
		return nil, syscall.ENOTDIR
	}

	id, err := n.client.LookupFileInDir(ctx, n.fileID, name)
	if err != nil {
		return nil, errToSyscall(err)
	}

	fileMeta, err := n.client.GetFile(ctx, id)
	if err != nil {
		return nil, errToSyscall(err)
	}

	out.SetAttrTimeout(time.Second)
	out.SetEntryTimeout(time.Second)

	if err := n.getAttrResponse(fileMeta, &out.Attr); err != nil {
		return nil, errToSyscall(err)
	}

	if fileMeta.Name == "" {
		log.Printf("uh oh, empty filename for %d inside %d with name %q", id, n.fileID, name)
	}

	stable := fs.StableAttr{
		Mode: fileMeta.Mode(),
		Ino:  uint64(id),
	}
	child := n.NewInode(ctx, &Node{
		fileID:   id,
		fileType: fileMeta.FileType,
		client:   n.client,
	}, stable)

	n.AddChild(fileMeta.Name, child, true)

	return child, 0
}

func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n.fileType != FileTypeDirectory {
		return nil, syscall.ENOTDIR
	}

	newFileID, err := n.client.CreateFile(ctx, n.fileID, name, FileTypeDirectory)
	if errors.Is(err, ErrNotSupported) {
		return nil, syscall.ENOTDIR
	} else if err != nil {
		return nil, errToSyscall(err)
	}

	stable := fs.StableAttr{
		Mode: 0o755 | fuse.S_IFDIR,
		Ino:  uint64(newFileID),
	}
	child := n.NewInode(ctx, &Node{
		fileID:   newFileID,
		fileType: FileTypeDirectory,
		client:   n.client,
	}, stable)

	n.AddChild(name, child, true)

	return child, 0
}

func (n *Node) Mknod(ctx context.Context, name string, mode uint32, dev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.ENOTSUP
}

func (n *Node) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (node *fs.Inode, errno syscall.Errno) {
	return nil, syscall.ENOTSUP
}

func (n *Node) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (node *fs.Inode, errno syscall.Errno) {
	return nil, syscall.ENOTSUP
}

func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if n.fileType != FileTypeDirectory {
		return nil, nil, 0, syscall.ENOENT
	}

	newFileID, err := n.client.CreateFile(ctx, n.fileID, name, FileTypeRegular)
	if errors.Is(err, ErrNotSupported) {
		return nil, nil, 0, syscall.ENOTDIR
	} else if err != nil {
		return nil, nil, 0, errToSyscall(err)
	}

	stable := fs.StableAttr{
		Mode: 0o644 | fuse.S_IFREG,
		Ino:  uint64(newFileID),
	}

	newNode := &Node{
		fileID:   newFileID,
		fileType: FileTypeRegular,
		client:   n.client,
	}
	child := n.NewInode(ctx, newNode, stable)

	n.AddChild(name, child, true)

	return child, &FileHandle{newNode}, fuse.FOPEN_DIRECT_IO, 0
}

func (n *Node) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if n.fileType != FileTypeRegular {
		return nil, 0, syscall.EISDIR
	}

	log.Printf("file ID %d just opened", n.fileID)

	newNode := &Node{
		fileID:   n.fileID,
		fileType: FileTypeRegular,
		client:   n.client,
	}

	return &FileHandle{newNode}, fuse.FOPEN_DIRECT_IO, 0
}

func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	if n.fileType != FileTypeDirectory {
		return syscall.ENOTDIR
	}

	id, err := n.client.LookupFileInDir(ctx, n.fileID, name)
	if err != nil {
		return errToSyscall(err)
	}

	err = n.client.DeleteFile(ctx, id)
	if errors.Is(err, ErrNotSupported) {
		return syscall.EISDIR
	}

	return errToSyscall(err)
}

func (n *Node) Opendir(ctx context.Context) syscall.Errno {
	return 0
}

func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if n.fileType != FileTypeDirectory {
		return nil, syscall.ENOTDIR
	}

	files, err := n.client.ReadDir(ctx, n.fileID)
	if err != nil {
		return nil, errToSyscall(err)
	}

	var results []fuse.DirEntry

	for _, file := range files {
		results = append(results, fuse.DirEntry{
			Mode: file.Mode(),
			Name: file.Name,
			Ino:  uint64(file.FileID),
		})
	}

	return fs.NewListDirStream(results), 0
}

func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	if n.fileType != FileTypeDirectory {
		return syscall.ENOTDIR
	}

	id, err := n.client.LookupFileInDir(ctx, n.fileID, name)
	if err != nil {
		return errToSyscall(err)
	}

	err = n.client.DeleteDir(ctx, id)
	if errors.Is(err, ErrNotSupported) {
		return syscall.ENOTDIR
	}

	return errToSyscall(err)
}

func (n *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if n.fileType != FileTypeDirectory {
		return syscall.ENOTDIR
	}

	newParentNode := newParent.(*Node)

	if newParentNode.fileType != FileTypeDirectory {
		return syscall.ENOTDIR
	}

	id, err := n.client.LookupFileInDir(ctx, n.fileID, name)
	if err != nil {
		return errToSyscall(err)
	}

	return errToSyscall(n.client.RenameFile(ctx, id, newParentNode.fileID, newName))
}

// func (n *Node) CopyFileRange(ctx context.Context, fhIn fs.FileHandle,
// 	offIn uint64, out *Inode, fhOut fs.FileHandle, offOut uint64,
// 	len uint64, flags uint64) (uint32, syscall.Errno) {

// }
