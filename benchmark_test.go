package sipflow

import (
 "context"
 "fmt"
 "testing"
 "time"
)

var benchmarkFlow Flow
var benchmarkEvent Event

func BenchmarkParseInvite(b *testing.B) {
 raw:=sipReq("INVITE","bench-call","z9hG4bK-bench","a","",1)
 m:=Meta{ObservedAt:time.Unix(1,0),Transport:TransportUDP}
 b.ReportAllocs(); b.SetBytes(int64(len(raw))); b.ResetTimer()
 for i:=0;i<b.N;i++ { e,err:=Parse(raw,Inbound,m,ParseOptions{}); if err!=nil{b.Fatal(err)}; benchmarkEvent=e }
}

func BenchmarkTrackerCompleteCall(b *testing.B) {
 base:=time.Unix(1,0)
 b.ReportAllocs(); b.ResetTimer()
 for i:=0;i<b.N;i++ {
  id:=fmt.Sprintf("bench-%d",i); tr:=NewTracker()
  raws:=[][]byte{
   sipReq("INVITE",id,"z9hG4bK-i","a","",1),
   sipResp(180,"Ringing",id,"z9hG4bK-i","a","b","INVITE",1),
   sipResp(200,"OK",id,"z9hG4bK-i","a","b","INVITE",1),
   sipReq("BYE",id,"z9hG4bK-b","b","a",2),
   sipResp(200,"OK",id,"z9hG4bK-b","b","a","BYE",2),
  }
  for j,raw:=range raws { e,err:=Parse(raw,Inbound,Meta{ObservedAt:base.Add(time.Duration(j)*time.Millisecond)},ParseOptions{});if err!=nil{b.Fatal(err)};benchmarkFlow,_=tr.Apply(e) }
 }
}

func BenchmarkRecorderObserveParallel(b *testing.B) {
 r:=New(WithQueueSize(65536),WithOverflowPolicy(Block))
 raw:=sipReq("OPTIONS","parallel","z9hG4bK-p","a","",1)
 b.ReportAllocs();b.ResetTimer()
 b.RunParallel(func(pb *testing.PB){for pb.Next(){if err:=r.Observe(context.Background(),Inbound,raw,Meta{});err!=nil{b.Error(err);return}}})
 b.StopTimer();ctx,cancel:=context.WithTimeout(context.Background(),30*time.Second);defer cancel();if err:=r.Close(ctx);err!=nil{b.Fatal(err)}
}
