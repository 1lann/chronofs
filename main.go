package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bep/debounce"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type LoopbackFileHandle interface {
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

type MinecraftFile struct {
	LoopbackFileHandle
	node *MinecraftNode
}

func (f *MinecraftFile) Release(ctx context.Context) syscall.Errno {
	log.Println("release called on:", f.node.Path(nil))
	return f.LoopbackFileHandle.Release(ctx)
}

func (f *MinecraftFile) Flush(ctx context.Context) syscall.Errno {
	log.Println("flush called on:", f.node.Path(nil))
	return f.LoopbackFileHandle.Flush(ctx)
}

func (f *MinecraftFile) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	// filePath := f.node.Path(nil)
	// switch filepath.Base(filePath) {
	// case "level.dat", "session.lock", "level.dat_old", "r.0.0.mca":
	// 	log.Println("fsync called on:", filePath)
	// }

	// if flags == 0 {
	// 	log.Println("fsync called on:", filePath)
	// }

	return 0

	// return f.LoopbackFileHandle.Fsync(ctx, flags)
}

var writeDebouncer = debounce.New(50 * time.Millisecond)
var writeCounter atomic.Int32
var lastOffset atomic.Int64
var lastBytesWritten atomic.Int64

func (f *MinecraftFile) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	filePath := f.node.Path(nil)
	switch filepath.Base(filePath) {
	case "level.dat", "session.lock", "level.dat_old":
		log.Println("write called on:", filePath)
	case "r.0.0.mca":
		writeCounter.Add(1)
		if off != 0 {
			lastOffset.Store(off)
			lastBytesWritten.Store(int64(len(data)))
		}
		writeDebouncer(func() {
			log.Println("wrote to r.0.0.mca", writeCounter.Load(), "times", "at offset", lastOffset.Load(), "length", lastBytesWritten.Load())
			writeCounter.Store(0)
		})
	}

	return f.LoopbackFileHandle.Write(ctx, data, off)
}

var readDebouncer = debounce.New(50 * time.Millisecond)
var readCounter atomic.Int32

func (f *MinecraftFile) Read(ctx context.Context, data []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	filePath := f.node.Path(nil)
	switch filepath.Base(filePath) {
	case "level.dat", "session.lock", "level.dat_old":
		log.Println("read called on:", filePath)
	case "r.0.0.mca":
		readCounter.Add(1)
		readDebouncer(func() {
			log.Println("read from r.0.0.mca", readCounter.Load(), "times")
			readCounter.Store(0)
		})
	}

	return f.LoopbackFileHandle.Read(ctx, data, off)
}

// MinecraftNode emulates Windows FS semantics, which forbids deleting open files.
type MinecraftNode struct {
	// MinecraftNode inherits most functionality from LoopbackNode.
	fs.LoopbackNode

	mu        sync.Mutex
	openCount int
}

var _ = (fs.NodeOpener)((*MinecraftNode)(nil))

func (n *MinecraftNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	fh, flags, errno := n.LoopbackNode.Open(ctx, flags)
	if errno == 0 {
		n.mu.Lock()
		defer n.mu.Unlock()

		n.openCount++
	}
	return &MinecraftFile{
		LoopbackFileHandle: fh.(LoopbackFileHandle),
		node:               n,
	}, flags, errno
}

var _ = (fs.NodeCreater)((*MinecraftNode)(nil))

func (n *MinecraftNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	inode, fh, flags, errno := n.LoopbackNode.Create(ctx, name, flags, mode, out)
	if errno == 0 {
		wn := inode.Operations().(*MinecraftNode)
		wn.openCount++
	}

	return inode, &MinecraftFile{
		LoopbackFileHandle: fh.(LoopbackFileHandle),
		node:               n,
	}, flags, errno
}

var _ = (fs.NodeReleaser)((*MinecraftNode)(nil))

func (n *MinecraftNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	log.Println("rename called from", name, " to ", newName)
	return n.LoopbackNode.Rename(ctx, name, newParent, newName, flags)
}

// Release decreases the open count. The kernel doesn't wait with
// returning from close(), so if the caller is too quick to
// unlink/rename after calling close(), this may still trigger EBUSY.
func (n *MinecraftNode) Release(ctx context.Context, f fs.FileHandle) syscall.Errno {
	log.Println("closing:", n.Path(nil))

	n.mu.Lock()
	defer n.mu.Unlock()

	n.openCount--
	if fr, ok := f.(fs.FileReleaser); ok {
		return fr.Release(ctx)
	}
	return 0
}

var _ = (fs.NodeUnlinker)((*MinecraftNode)(nil))

func (n *MinecraftNode) Unlink(ctx context.Context, name string) syscall.Errno {
	return n.LoopbackNode.Unlink(ctx, name)
}

func newMinecraftNode(rootData *fs.LoopbackRoot, parent *fs.Inode, name string, st *syscall.Stat_t) fs.InodeEmbedder {
	n := &MinecraftNode{
		LoopbackNode: fs.LoopbackNode{
			RootData: rootData,
		},
	}
	return n
}

// ExampleLoopbackReuse shows how to build a file system on top of the
// loopback file system.
func main() {
	mntDir := "/home/jason/Workspace/testserver/serverdata"
	origDir := "/home/jason/Workspace/testserver/origdata"

	rootData := &fs.LoopbackRoot{
		NewNode: newMinecraftNode,
		Path:    origDir,
	}

	sec := time.Second
	opts := &fs.Options{
		AttrTimeout:  &sec,
		EntryTimeout: &sec,
	}

	server, err := fs.Mount(mntDir, newMinecraftNode(rootData, nil, "", nil), opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	fmt.Printf("files under %s cannot be deleted if they are opened", mntDir)
	server.Wait()
}
