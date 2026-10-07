package memory

import ("context";"sync";"github.com/haarardin/sipflow")
type Sink struct{ mu sync.RWMutex; events []sipflow.Event; completed []sipflow.Flow }
func New()*Sink{return &Sink{}}
func(s *Sink)WriteEvent(_ context.Context,e sipflow.Event)error{s.mu.Lock();defer s.mu.Unlock();s.events=append(s.events,e);return nil}
func(s *Sink)CompleteFlow(_ context.Context,f sipflow.Flow)error{s.mu.Lock();defer s.mu.Unlock();s.completed=append(s.completed,f);return nil}
func(s *Sink)Events()[]sipflow.Event{s.mu.RLock();defer s.mu.RUnlock();return append([]sipflow.Event(nil),s.events...)}
func(s *Sink)CompletedFlows()[]sipflow.Flow{s.mu.RLock();defer s.mu.RUnlock();return append([]sipflow.Flow(nil),s.completed...)}
