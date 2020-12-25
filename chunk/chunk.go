package chunk

import (
	"context"
	"io"
)

type CtxKey string

type Reader interface {
	ReadAt(ctx context.Context, p *Page, off int) (int, error)
}

type Writer interface {
	io.WriterAt
	ID() uint64
	FlushTo(offset int) error
	SetID(chunkid uint64)
	Finish(length int) error
	Abort()
}

type ChunkStore interface {
	NewReader(chunkid uint64, length int) Reader
	NewWriter(chunkid uint64) Writer
	Remove(chunkid uint64, length int) error
}
