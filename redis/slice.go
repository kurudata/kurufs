package redis

type slice struct {
	chunkid uint64
	cleng   uint32
	off     uint32
	len     uint32
	pos     uint32
	left    *slice
	right   *slice
}

func newSlice(pos uint32, chunkid uint64, cleng, off, len uint32) *slice {
	if len == 0 {
		return nil
	}
	s := &slice{}
	s.pos = pos
	s.chunkid = chunkid
	s.cleng = cleng
	s.off = off
	s.len = len
	s.left = nil
	s.right = nil
	return s
}

func (s *slice) cut(pos uint32) (left, right *slice) {
	if s == nil {
		return nil, nil
	}
	if pos <= s.pos {
		if s.left == nil {
			s.left = newSlice(pos, 0, 0, 0, s.pos-pos)
		}
		left, s.left = s.left.cut(pos)
		return left, s
	} else if pos < s.pos+s.len {
		l := pos - s.pos
		right = newSlice(pos, s.chunkid, s.cleng, s.off+l, s.len-l)
		right.right = s.right
		s.len = l
		s.right = nil
		return s, right
	} else {
		if s.right == nil {
			s.right = newSlice(s.pos+s.len, 0, 0, 0, pos-s.pos-s.len)
		}
		s.right, right = s.right.cut(pos)
		return s, right
	}
}

func (s *slice) visit(f func(*slice)) {
	if s == nil {
		return
	}
	s.left.visit(f)
	right := s.right
	f(s) // s could be freed
	right.visit(f)
}
