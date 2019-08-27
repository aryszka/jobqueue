package jobqueue

import "container/list"

type stack struct {
	cap  int
	list *list.List
}

func newStack(cap int) *stack {
	return &stack{
		cap:  cap,
		list: list.New(),
	}
}

func (s *stack) empty() bool {
	return s.list.Len() == 0
}

func (s *stack) full() bool {
	return s.cap > 0 && s.list.Len() == s.cap
}

func (s *stack) bottom() *job {
	if s.list.Len() == 0 {
		return nil
	}

	return s.list.Back().Value.(*job)
}

func (s *stack) push(j *job) {
	j.entry = s.list.PushFront(j)
}

func (s *stack) removeEntry(e *list.Element) *job {
	s.list.Remove(e)
	j := e.Value.(*job)
	j.entry = nil
	return j
}

func (s *stack) pop() *job {
	return s.removeEntry(s.list.Front())
}

func (s *stack) shift() *job {
	return s.removeEntry(s.list.Back())
}
