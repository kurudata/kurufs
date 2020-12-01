package meta

type Ino uint64

type Context interface {
	Gid() uint32
	Uid() uint32
	Pid() uint32
	Cancel()
	Canceled() bool
}

type emptyContext struct{}

func (ctx emptyContext) Gid() uint32    { return 0 }
func (ctx emptyContext) Uid() uint32    { return 0 }
func (ctx emptyContext) Pid() uint32    { return 1 }
func (ctx emptyContext) Cancel()        {}
func (ctx emptyContext) Canceled() bool { return false }

var Background Context = emptyContext{}
