package sipflow

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type TrackerOptions struct {
	TerminalRetention time.Duration
	InactiveRetention time.Duration
}

type TrackerStats struct {
	Flows        int
	Fingerprints int
}

type Tracker struct {
	mu      sync.RWMutex
	flows   map[string]*Flow
	seq     uint64
	seen    map[string]seenEntry
	options TrackerOptions
}

type seenEntry struct {
	flowID string
}

func NewTracker() *Tracker { return NewTrackerWithOptions(TrackerOptions{}) }

func NewTrackerWithOptions(options TrackerOptions) *Tracker {
	return &Tracker{flows: map[string]*Flow{}, seen: map[string]seenEntry{}, options: options}
}

func (t *Tracker) Apply(e Event) (Flow, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.seq++
	e.Sequence = t.seq
	id := e.CorrelationID
	if id == "" {
		id = e.CallID
	}
	f := t.flows[id]
	if f == nil {
		f = &Flow{ID: id, State: StateUnknown, FirstSeenAt: e.ObservedAt, Transactions: map[string]*Transaction{}, Dialogs: map[string]*Dialog{}}
		t.flows[id] = f
	}
	terminal := isTerminal(f.State)
	f.LastSeenAt = e.ObservedAt
	if !has(f.CallIDs, e.CallID) {
		f.CallIDs = append(f.CallIDs, e.CallID)
		sort.Strings(f.CallIDs)
	}

	fp := fmt.Sprintf("%s|%s|%s|%d|%s|%s|%d|%s|%s", e.Direction, e.CallID, e.Kind, e.StatusCode, e.Method, e.CSeqMethod, e.CSeq, e.TopViaBranch, e.RequestURI)
	if _, ok := t.seen[fp]; ok {
		e.Retransmission = true
		f.Retransmissions++
	} else {
		t.seen[fp] = seenEntry{flowID: id}
	}

	method := e.CSeqMethod
	if method == "" {
		method = e.Method
	}
	tk := fmt.Sprintf("%s|%s|%d|%s", e.CallID, e.TopViaBranch, e.CSeq, strings.ToUpper(method))
	if method != "" {
		tx := f.Transactions[tk]
		if tx == nil {
			tx = &Transaction{Key: tk, CallID: e.CallID, Branch: e.TopViaBranch, Method: method, CSeq: e.CSeq, FirstSeenAt: e.ObservedAt}
			f.Transactions[tk] = tx
		}
		tx.LastSeenAt = e.ObservedAt
		tx.EventSequences = append(tx.EventSequences, e.Sequence)
		if e.Retransmission {
			tx.Retransmissions++
		}
		if e.Kind == ResponseKind && e.StatusCode >= 200 {
			tx.FinalStatus = e.StatusCode
		}
	}

	if e.FromTag != "" && e.ToTag != "" {
		a, b := e.FromTag, e.ToTag
		if a > b {
			a, b = b, a
		}
		dk := e.CallID + "|" + a + "|" + b
		d := f.Dialogs[dk]
		if d == nil {
			d = &Dialog{Key: dk, CallID: e.CallID, TagA: a, TagB: b, FirstSeenAt: e.ObservedAt}
			f.Dialogs[dk] = d
		}
		d.LastSeenAt = e.ObservedAt
		d.EventSequences = append(d.EventSequences, e.Sequence)
	}

	f.Events = append(f.Events, e)
	state(f, e)
	metrics(f)
	return clone(f), !terminal && isTerminal(f.State)
}

func (t *Tracker) Flow(id string) (Flow, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	f, ok := t.flows[id]
	if !ok {
		return Flow{}, false
	}
	return clone(f), true
}

func (t *Tracker) Flows() []Flow {
	t.mu.RLock()
	defer t.mu.RUnlock()
	o := make([]Flow, 0, len(t.flows))
	for _, f := range t.flows {
		o = append(o, clone(f))
	}
	sort.Slice(o, func(i, j int) bool { return o[i].FirstSeenAt.Before(o[j].FirstSeenAt) })
	return o
}

func (t *Tracker) Cleanup(now time.Time) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	removed := 0
	for id, f := range t.flows {
		var retention time.Duration
		if isTerminal(f.State) {
			retention = t.options.TerminalRetention
		} else {
			retention = t.options.InactiveRetention
		}
		if retention <= 0 || now.Sub(f.LastSeenAt) < retention {
			continue
		}
		delete(t.flows, id)
		for fp, entry := range t.seen {
			if entry.flowID == id {
				delete(t.seen, fp)
			}
		}
		removed++
	}
	return removed
}

func (t *Tracker) Stats() TrackerStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return TrackerStats{Flows: len(t.flows), Fingerprints: len(t.seen)}
}

func state(f *Flow, e Event) {
	m := strings.ToUpper(e.CSeqMethod)
	if e.Kind == RequestKind {
		m = strings.ToUpper(e.Method)
	}
	if e.Kind == RequestKind && m == "INVITE" && f.StartedAt.IsZero() {
		f.StartedAt = e.ObservedAt
		f.State = StateInviting
	}
	if e.Kind == ResponseKind && m == "INVITE" {
		switch {
		case e.StatusCode >= 100 && e.StatusCode < 200:
			if f.firstProvisionalAt == nil {
				x := e.ObservedAt
				f.firstProvisionalAt = &x
			}
			if e.StatusCode == 180 {
				if f.ringingAt == nil {
					x := e.ObservedAt
					f.ringingAt = &x
				}
				f.State = StateRinging
			}
			if e.StatusCode == 183 {
				f.EarlyMedia = true
				f.State = StateEarlyMedia
			}
		case e.StatusCode >= 200 && e.StatusCode < 300:
			if f.AnsweredAt == nil {
				x := e.ObservedAt
				f.AnsweredAt = &x
			}
			f.FinalStatus = e.StatusCode
			f.FinalReason = e.Reason
			f.State = StateEstablished
		case e.StatusCode >= 300:
			f.FinalStatus = e.StatusCode
			f.FinalReason = e.Reason
			if f.Cancelled || e.StatusCode == 487 {
				f.State = StateCancelled
			} else if f.AnsweredAt == nil {
				f.State = StateFailed
			}
			if isTerminal(f.State) && f.EndedAt == nil {
				x := e.ObservedAt
				f.EndedAt = &x
			}
		}
	}
	if e.Kind == RequestKind && m == "CANCEL" {
		f.Cancelled = true
		if f.AnsweredAt == nil {
			f.State = StateCancelled
		}
	}
	if e.Kind == RequestKind && m == "BYE" {
		f.State = StateTerminating
		if f.EndedAt == nil {
			x := e.ObservedAt
			f.EndedAt = &x
		}
	}
	if e.Kind == ResponseKind && m == "BYE" && e.StatusCode >= 200 {
		f.State = StateEnded
		f.FinalStatus = e.StatusCode
		f.FinalReason = e.Reason
		if f.EndedAt == nil {
			x := e.ObservedAt
			f.EndedAt = &x
		}
	}
}

func metrics(f *Flow) {
	if f.StartedAt.IsZero() {
		return
	}
	if f.firstProvisionalAt != nil {
		f.Metrics.FirstProvisionalMS = f.firstProvisionalAt.Sub(f.StartedAt).Milliseconds()
	}
	if f.ringingAt != nil {
		f.Metrics.RingingMS = f.ringingAt.Sub(f.StartedAt).Milliseconds()
	}
	if f.AnsweredAt != nil {
		f.Metrics.SetupMS = f.AnsweredAt.Sub(f.StartedAt).Milliseconds()
	}
	if f.AnsweredAt != nil && f.EndedAt != nil && f.EndedAt.After(*f.AnsweredAt) {
		f.Metrics.TalkMS = f.EndedAt.Sub(*f.AnsweredAt).Milliseconds()
	}
}

func isTerminal(s FlowState) bool { return s == StateEnded || s == StateFailed || s == StateCancelled }
func has(v []string, w string) bool {
	for _, x := range v {
		if x == w {
			return true
		}
	}
	return false
}
func clone(f *Flow) Flow {
	o := *f
	o.CallIDs = append([]string(nil), f.CallIDs...)
	o.Events = append([]Event(nil), f.Events...)
	o.Transactions = map[string]*Transaction{}
	for k, v := range f.Transactions {
		x := *v
		x.EventSequences = append([]uint64(nil), v.EventSequences...)
		o.Transactions[k] = &x
	}
	o.Dialogs = map[string]*Dialog{}
	for k, v := range f.Dialogs {
		x := *v
		x.EventSequences = append([]uint64(nil), v.EventSequences...)
		o.Dialogs[k] = &x
	}
	return o
}
