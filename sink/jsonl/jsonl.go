package jsonl
import("context";"encoding/json";"io";"sync";"github.com/haarardin/sipflow")
type Sink struct{mu sync.Mutex;w io.Writer}
func New(w io.Writer)*Sink{return &Sink{w:w}}
func(s *Sink)WriteEvent(_ context.Context,e sipflow.Event)error{return s.write(map[string]any{"type":"sip_event","event":e})}
func(s *Sink)CompleteFlow(_ context.Context,f sipflow.Flow)error{return s.write(map[string]any{"type":"sip_flow_completed","flow":f})}
func(s *Sink)write(v any)error{s.mu.Lock();defer s.mu.Unlock();return json.NewEncoder(s.w).Encode(v)}
