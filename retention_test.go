package sipflow

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestTrackerCleanupTerminalFlowAndFingerprints(t *testing.T) {
	retention := 10 * time.Second
	tr := NewTrackerWithOptions(TrackerOptions{TerminalRetention: retention})
	base := time.Unix(1000, 0)
	raws := [][]byte{
		sipReq("INVITE", "cleanup", "z9hG4bK-i", "a", "", 1),
		sipResp(200, "OK", "cleanup", "z9hG4bK-i", "a", "b", "INVITE", 1),
		sipReq("BYE", "cleanup", "z9hG4bK-b", "b", "a", 2),
		sipResp(200, "OK", "cleanup", "z9hG4bK-b", "b", "a", "BYE", 2),
	}
	for i, raw := range raws {
		e, err := Parse(raw, Inbound, Meta{ObservedAt: base.Add(time.Duration(i) * time.Second)}, ParseOptions{})
		if err != nil { t.Fatal(err) }
		tr.Apply(e)
	}
	before := tr.Stats()
	if before.Flows != 1 || before.Fingerprints != 4 { t.Fatalf("before=%+v", before) }
	if removed := tr.Cleanup(base.Add(12 * time.Second)); removed != 0 { t.Fatalf("removed too early=%d", removed) }
	if removed := tr.Cleanup(base.Add(14 * time.Second)); removed != 1 { t.Fatalf("removed=%d", removed) }
	after := tr.Stats()
	if after.Flows != 0 || after.Fingerprints != 0 { t.Fatalf("after=%+v", after) }

	e, err := Parse(raws[0], Inbound, Meta{ObservedAt: base.Add(20 * time.Second)}, ParseOptions{})
	if err != nil { t.Fatal(err) }
	f, _ := tr.Apply(e)
	if f.Events[0].Retransmission { t.Fatal("evicted fingerprint leaked into new flow") }
}

func TestTrackerCleanupInactiveFlow(t *testing.T) {
	tr := NewTrackerWithOptions(TrackerOptions{InactiveRetention: 5 * time.Second})
	base := time.Unix(2000, 0)
	e, _ := Parse(sipReq("INVITE", "stale", "z9hG4bK-s", "a", "", 1), Inbound, Meta{ObservedAt: base}, ParseOptions{})
	tr.Apply(e)
	if removed := tr.Cleanup(base.Add(4 * time.Second)); removed != 0 { t.Fatalf("removed early=%d", removed) }
	if removed := tr.Cleanup(base.Add(5 * time.Second)); removed != 1 { t.Fatalf("removed=%d", removed) }
}

func TestTrackerCleanupDisabledByDefault(t *testing.T) {
	tr := NewTracker()
	base := time.Unix(3000, 0)
	e, _ := Parse(sipReq("OPTIONS", "keep", "z9hG4bK-k", "a", "", 1), Inbound, Meta{ObservedAt: base}, ParseOptions{})
	tr.Apply(e)
	if removed := tr.Cleanup(base.Add(365 * 24 * time.Hour)); removed != 0 { t.Fatalf("removed=%d", removed) }
	if tr.Stats().Flows != 1 { t.Fatal("default cleanup must preserve compatibility") }
}

func TestRecorderAutomaticCleanup(t *testing.T) {
	r := New(WithInactiveRetention(10*time.Millisecond), WithCleanupInterval(5*time.Millisecond), WithQueueSize(16), WithOverflowPolicy(Block))
	if err := r.Observe(context.Background(), Inbound, sipReq("OPTIONS", "auto", "z9hG4bK-a", "a", "", 1), Meta{}); err != nil { t.Fatal(err) }
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if r.Stats().Flows == 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := r.Close(ctx); err != nil { t.Fatal(err) }
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("flow was not automatically evicted: %+v", r.Stats())
}

func stressBoundedRetention(t *testing.T) {
	if testing.Short() { t.Skip("stress test") }
	const batches = 10
	const perBatch = 10000
	tr := NewTrackerWithOptions(TrackerOptions{TerminalRetention: time.Second})
	base := time.Unix(10000, 0)
	for batch := 0; batch < batches; batch++ {
		at := base.Add(time.Duration(batch) * 10 * time.Second)
		for i := 0; i < perBatch; i++ {
			id := fmt.Sprintf("bounded-%d-%d", batch, i)
			seq := [][]byte{
				sipReq("INVITE", id, "z9hG4bK-i", "a", "", 1),
				sipResp(200, "OK", id, "z9hG4bK-i", "a", "b", "INVITE", 1),
				sipReq("BYE", id, "z9hG4bK-b", "b", "a", 2),
				sipResp(200, "OK", id, "z9hG4bK-b", "b", "a", "BYE", 2),
			}
			for j, raw := range seq {
				e, err := Parse(raw, Inbound, Meta{ObservedAt: at.Add(time.Duration(j) * time.Millisecond)}, ParseOptions{})
				if err != nil { t.Fatal(err) }
				tr.Apply(e)
			}
		}
		if got := tr.Stats().Flows; got != perBatch { t.Fatalf("batch %d flows=%d want=%d", batch, got, perBatch) }
		if removed := tr.Cleanup(at.Add(2 * time.Second)); removed != perBatch { t.Fatalf("batch %d removed=%d", batch, removed) }
		stats := tr.Stats()
		if stats.Flows != 0 || stats.Fingerprints != 0 { t.Fatalf("batch %d retained state: %+v", batch, stats) }
	}
}
