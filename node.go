package chronofs

import (
	"context"
	"log"
	"os/user"
	"strconv"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type NodeContext struct {
	FSClient
	user         *user.User
	fsyncTimeout func(*FileMeta) time.Duration
}

type Node struct {
	fs.Inode
	fileID   int64
	fileType FileType
	context  *NodeContext
}

func NewRootNode(client FSClient, user *user.User,
	fsyncTimeout func(*FileMeta) time.Duration) *Node {
	return NewNodeWithRoot(client, user, RootID, fsyncTimeout)
}

func NewNodeWithRoot(client FSClient, user *user.User, rootID int64,
	fsyncTimeout func(*FileMeta) time.Duration) *Node {
	return &Node{
		fileID:   rootID,
		fileType: FileTypeDirectory,
		context: &NodeContext{
			FSClient:     client,
			user:         user,
			fsyncTimeout: fsyncTimeout,
		},
	}
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

func getUserGroupID(u *user.User) (uid int64, gid int64, err error) {
	uid, err = strconv.ParseInt(u.Uid, 10, 64)
	if err != nil {
		return 0, 0, err
	}

	gid, err = strconv.ParseInt(u.Gid, 10, 64)
	if err != nil {
		return 0, 0, err
	}

	return uid, gid, nil
}

func (n *Node) getAttrResponse(file *FileMeta, out *fuse.Attr) syscall.Errno {
	out.Nlink = 1
	out.Mode = file.Mode()
	out.Size = uint64(file.Length)

	if file.FileType == FileTypeDirectory {
		out.Size = 4096
	} else if file.FileType == FileTypeSymlink {
		out.Size = uint64(len(file.Link))
	}

	out.Blocks = uint64(file.Length >> 9)
	out.Atime = uint64(file.LastAccess.Unix())
	out.Mtime = uint64(file.LastWrite.Unix())
	out.Atimensec = uint32(file.LastAccess.Nanosecond())
	out.Mtimensec = uint32(file.LastWrite.Nanosecond())

	uid, gid, err := getUserGroupID(n.context.user)
	if err != nil {
		log.Printf("failed to get user group id: %+v", err)
		return syscall.EIO
	}

	out.Owner = fuse.Owner{
		Uid: uint32(uid),
		Gid: uint32(gid),
	}

	out.Blksize = 4096
	out.Padding = 0

	return 0
}

func (n *Node) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	file, err := n.context.GetFile(ctx, n.fileID)
	if err != nil {
		return errToSyscall(err)
	}

	out.SetTimeout(time.Second)
	if err := n.getAttrResponse(file, &out.Attr); err != 0 {
		return err
	}

	return 0
}

func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if size, ok := in.GetSize(); ok {
		err := n.context.SetFileLength(ctx, n.fileID, int64(size))
		if err != nil {
			return errToSyscall(err)
		}
	}

	err := n.context.SetFileAttrs(ctx, n.fileID, func(fileMeta *FileMeta) {
		if atime, ok := in.GetATime(); ok {
			fileMeta.LastAccess = atime
		}
		if mtime, ok := in.GetMTime(); ok {
			fileMeta.LastWrite = mtime
		}
		if mode, ok := in.GetMode(); ok {
			fileMeta.Permissions = int64(mode)
		}
		if uid, ok := in.GetUID(); ok {
			fileMeta.Owner = int64(uid)
		}
		if gid, ok := in.GetGID(); ok {
			fileMeta.Group = int64(gid)
		}
	})
	if err != nil {
		return errToSyscall(err)
	}

	return n.Getattr(ctx, f, out)
}

func (n *Node) OnAdd(ctx context.Context) {}

// 	Readlink(ctx context.Context) ([]byte, syscall.Errno)

func (n *Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	file, err := n.context.GetFile(ctx, n.fileID)
	if err != nil {
		return nil, errToSyscall(err)
	}

	if file.FileType != FileTypeSymlink {
		return nil, syscall.EINVAL
	}

	return []byte(file.Link), 0
}

func errToSyscall(err error) syscall.Errno {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	} else if errors.Is(err, ErrNotFound) {
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

	id, err := n.context.LookupFileInDir(ctx, n.fileID, name)
	if err != nil {
		return nil, errToSyscall(err)
	}

	fileMeta, err := n.context.GetFile(ctx, id)
	if err != nil {
		return nil, errToSyscall(err)
	}

	out.SetAttrTimeout(time.Second)
	out.SetEntryTimeout(time.Second)

	if err := n.getAttrResponse(fileMeta, &out.Attr); err != 0 {
		return nil, err
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
		context:  n.context,
	}, stable)

	n.AddChild(fileMeta.Name, child, true)

	return child, 0
}

func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n.fileType != FileTypeDirectory {
		return nil, syscall.ENOTDIR
	}

	caller, ok := fuse.FromContext(ctx)
	if !ok {
		log.Println("no caller in context")
		return nil, syscall.EIO
	}

	newFileID, err := n.context.CreateFile(ctx, n.fileID, name, int64(mode), FileTypeDirectory,
		"", int64(caller.Uid), int64(caller.Gid))
	if errors.Is(err, ErrNotSupported) {
		return nil, syscall.ENOTDIR
	} else if err != nil {
		return nil, errToSyscall(err)
	}

	fileMeta, err := n.context.GetFile(ctx, newFileID)
	if err != nil {
		return nil, errToSyscall(err)
	}

	if err := n.getAttrResponse(fileMeta, &out.Attr); err != 0 {
		return nil, err
	}

	stable := fs.StableAttr{
		Mode: fileMeta.Mode(),
		Ino:  uint64(newFileID),
	}
	newNode := &Node{
		fileID:   fileMeta.FileID,
		fileType: fileMeta.FileType,
		context:  n.context,
	}
	child := n.NewInode(ctx, newNode, stable)

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
	if n.fileType != FileTypeDirectory {
		return nil, syscall.ENOENT
	}

	caller, ok := fuse.FromContext(ctx)
	if !ok {
		log.Println("no caller in context")
		return nil, syscall.EIO
	}

	newFileID, err := n.context.CreateFile(ctx, n.fileID, name, int64(0o777), FileTypeSymlink,
		target, int64(caller.Uid), int64(caller.Gid))
	if errors.Is(err, ErrNotSupported) {
		return nil, syscall.ENOTDIR
	} else if err != nil {
		return nil, errToSyscall(err)
	}

	fileMeta, err := n.context.GetFile(ctx, newFileID)
	if err != nil {
		return nil, errToSyscall(err)
	}

	if err := n.getAttrResponse(fileMeta, &out.Attr); err != 0 {
		return nil, err
	}

	stable := fs.StableAttr{
		Mode: fileMeta.Mode(),
		Ino:  uint64(newFileID),
	}
	newNode := &Node{
		fileID:   fileMeta.FileID,
		fileType: fileMeta.FileType,
		context:  n.context,
	}
	child := n.NewInode(ctx, newNode, stable)

	n.AddChild(name, child, true)

	return child, 0
}

func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if n.fileType != FileTypeDirectory {
		return nil, nil, 0, syscall.ENOENT
	}

	caller, ok := fuse.FromContext(ctx)
	if !ok {
		log.Println("no caller in context")
		return nil, nil, 0, syscall.EIO
	}

	newFileID, err := n.context.CreateFile(ctx, n.fileID, name, int64(mode), FileTypeRegular, "",
		int64(caller.Uid), int64(caller.Gid))
	if errors.Is(err, ErrNotSupported) {
		return nil, nil, 0, syscall.ENOTDIR
	} else if err != nil {
		return nil, nil, 0, errToSyscall(err)
	}

	fileMeta, err := n.context.GetFile(ctx, newFileID)
	if err != nil {
		return nil, nil, 0, errToSyscall(err)
	}

	if err := n.getAttrResponse(fileMeta, &out.Attr); err != 0 {
		return nil, nil, 0, err
	}

	stable := fs.StableAttr{
		Mode: fileMeta.Mode(),
		Ino:  uint64(newFileID),
	}
	newNode := &Node{
		fileID:   fileMeta.FileID,
		fileType: fileMeta.FileType,
		context:  n.context,
	}
	child := n.NewInode(ctx, newNode, stable)

	n.AddChild(name, child, true)

	return child, &FileHandle{newNode}, fuse.FOPEN_DIRECT_IO, 0
}

func (n *Node) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if n.fileType != FileTypeRegular {
		return nil, 0, syscall.EISDIR
	}

	return &FileHandle{n}, fuse.FOPEN_DIRECT_IO, 0
}

func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	if n.fileType != FileTypeDirectory {
		return syscall.ENOTDIR
	}

	id, err := n.context.LookupFileInDir(ctx, n.fileID, name)
	if err != nil {
		return errToSyscall(err)
	}

	err = n.context.DeleteFile(ctx, id)
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

	files, err := n.context.ReadDir(ctx, n.fileID)
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

	id, err := n.context.LookupFileInDir(ctx, n.fileID, name)
	if err != nil {
		return errToSyscall(err)
	}

	err = n.context.DeleteDir(ctx, id)
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

	id, err := n.context.LookupFileInDir(ctx, n.fileID, name)
	if err != nil {
		return errToSyscall(err)
	}

	return errToSyscall(n.context.RenameFile(ctx, id, newParentNode.fileID, newName))
}

// func (n *Node) CopyFileRange(ctx context.Context, fhIn fs.FileHandle,
// 	offIn uint64, out *Inode, fhOut fs.FileHandle, offOut uint64,
// 	len uint64, flags uint64) (uint32, syscall.Errno) {

// }
