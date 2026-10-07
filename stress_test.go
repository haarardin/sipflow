package sipflow

import (
 "context"
 "fmt"
 "runtime"
 "sync"
 "sync/atomic"
 "testing"
 "time"
)

func TestStressConcurrentCompleteCalls(t *testing.T) {
 if testing.Short(){t.Skip("stress test")}
 const calls=10000
 r:=New(WithQueueSize(32768),WithOverflowPolicy(Block))
 var accepted atomic.Int64
 workers:=runtime.GOMAXPROCS(0)*2
 jobs:=make(chan int,workers)
 var wg sync.WaitGroup
 for w:=0;w<workers;w++ { wg.Add(1);go func(){defer wg.Done();for i:=range jobs{
  id:=fmt.Sprintf("stress-%d",i);base:=time.Unix(int64(i+1),0)
  seq:=[][]byte{
   sipReq("INVITE",id,"z9hG4bK-i","a","",1),
   sipResp(180,"Ringing",id,"z9hG4bK-i","a","b","INVITE",1),
   sipResp(200,"OK",id,"z9hG4bK-i","a","b","INVITE",1),
   sipReq("BYE",id,"z9hG4bK-b","b","a",2),
   sipResp(200,"OK",id,"z9hG4bK-b","b","a","BYE",2),
  }
  for j,raw:=range seq {ctx,cancel:=context.WithTimeout(context.Background(),10*time.Second);err:=r.Observe(ctx,Inbound,raw,Meta{ObservedAt:base.Add(time.Duration(j)*time.Millisecond)});cancel();if err!=nil{t.Errorf("observe %s/%d: %v",id,j,err);return};accepted.Add(1)}
 }}()}
 for i:=0;i<calls;i++{jobs<-i};close(jobs);wg.Wait()
 ctx,cancel:=context.WithTimeout(context.Background(),60*time.Second);defer cancel();if err:=r.Close(ctx);err!=nil{t.Fatal(err)}
 if got:=accepted.Load();got!=calls*5{t.Fatalf("accepted=%d want=%d",got,calls*5)}
 flows:=r.Flows();if len(flows)!=calls{t.Fatalf("flows=%d want=%d",len(flows),calls)}
 for _,f:=range flows {if f.State!=StateEnded||len(f.Events)!=5||len(f.Transactions)!=2{t.Fatalf("invalid flow %s: state=%s events=%d tx=%d",f.ID,f.State,len(f.Events),len(f.Transactions))}}
}

func TestStressDropNewestSaturationRemainsResponsive(t *testing.T) {
 if testing.Short(){t.Skip("stress test")}
 s:=&blockingSink{gate:make(chan struct{}),entered:make(chan struct{})}
 r:=New(WithQueueSize(64),WithSink(s),WithOverflowPolicy(DropNewest))
 raw:=sipReq("OPTIONS","sat","z9hG4bK-s","a","",1)
 _=r.Observe(context.Background(),Inbound,raw,Meta{})
 select{case<-s.entered:case<-time.After(time.Second):t.Fatal("sink not entered")}
 start:=time.Now();var full int
 for i:=0;i<100000;i++{if err:=r.Observe(context.Background(),Inbound,raw,Meta{});err==ErrQueueFull{full++}}
 if elapsed:=time.Since(start);elapsed>5*time.Second{t.Fatalf("saturation path too slow: %v",elapsed)}
 if full==0{t.Fatal("expected dropped observations")}
 close(s.gate);ctx,cancel:=context.WithTimeout(context.Background(),10*time.Second);defer cancel();if err:=r.Close(ctx);err!=nil{t.Fatal(err)}
}
