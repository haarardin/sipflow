package sipflow

import (
 "context"
 "errors"
 "fmt"
 "sync"
 "sync/atomic"
 "testing"
 "time"
)

func sipReq(method, callID, branch, fromTag, toTag string, cseq int) []byte {
 to := "<sip:bob@example.com>"; if toTag!="" { to += ";tag="+toTag }
 return []byte(fmt.Sprintf("%s sip:bob@example.com SIP/2.0\r\nVia: SIP/2.0/UDP a.example.com;branch=%s\r\nFrom: <sip:alice@example.com>;tag=%s\r\nTo: %s\r\nCall-ID: %s\r\nCSeq: %d %s\r\nContent-Length: 0\r\n\r\n",method,branch,fromTag,to,callID,cseq,method))
}
func sipResp(code int, reason, callID, branch, fromTag, toTag, method string, cseq int) []byte {
 return []byte(fmt.Sprintf("SIP/2.0 %d %s\r\nVia: SIP/2.0/UDP a.example.com;branch=%s\r\nFrom: <sip:alice@example.com>;tag=%s\r\nTo: <sip:bob@example.com>;tag=%s\r\nCall-ID: %s\r\nCSeq: %d %s\r\nContent-Length: 0\r\n\r\n",code,reason,branch,fromTag,toTag,callID,cseq,method))
}

func TestParseRejectsMalformedAndMissingCallID(t *testing.T) {
 if _,err:=Parse([]byte("garbage"),Inbound,Meta{},ParseOptions{}); !errors.Is(err,ErrInvalidMessage){t.Fatalf("want invalid message, got %v",err)}
 raw:=[]byte("OPTIONS sip:bob@example.com SIP/2.0\r\nCSeq: 1 OPTIONS\r\n\r\n")
 if _,err:=Parse(raw,Inbound,Meta{},ParseOptions{}); !errors.Is(err,ErrMissingCallID){t.Fatalf("want missing Call-ID, got %v",err)}
}

func TestTrackerSuccessfulCallAndRetransmission(t *testing.T) {
 tr:=NewTracker(); base:=time.Unix(100,0)
 events:=[]struct{raw []byte; at time.Time}{
  {sipReq("INVITE","call-1","z9hG4bK-1","a","",1),base},
  {sipResp(180,"Ringing","call-1","z9hG4bK-1","a","b","INVITE",1),base.Add(200*time.Millisecond)},
  {sipResp(180,"Ringing","call-1","z9hG4bK-1","a","b","INVITE",1),base.Add(300*time.Millisecond)},
  {sipResp(200,"OK","call-1","z9hG4bK-1","a","b","INVITE",1),base.Add(2*time.Second)},
  {sipReq("BYE","call-1","z9hG4bK-2","b","a",2),base.Add(12*time.Second)},
  {sipResp(200,"OK","call-1","z9hG4bK-2","b","a","BYE",2),base.Add(12*time.Second+20*time.Millisecond)},
 }
 var f Flow
 for _,x:=range events { e,err:=Parse(x.raw,Inbound,Meta{ObservedAt:x.at},ParseOptions{});if err!=nil{t.Fatal(err)};f,_=tr.Apply(e)}
 if f.State!=StateEnded{t.Fatalf("state=%s",f.State)}
 if f.Retransmissions!=1{t.Fatalf("retransmissions=%d",f.Retransmissions)}
 if f.Metrics.RingingMS!=200||f.Metrics.SetupMS!=2000||f.Metrics.TalkMS!=10000{t.Fatalf("metrics=%+v",f.Metrics)}
 if len(f.Dialogs)!=1{t.Fatalf("dialogs=%d",len(f.Dialogs))}
 if len(f.Transactions)!=2{t.Fatalf("transactions=%d",len(f.Transactions))}
}

func TestTrackerB2BUACorrelationJoinsDifferentCallIDs(t *testing.T) {
 tr:=NewTracker(); at:=time.Now()
 for i,id:=range []string{"leg-a","leg-b"} { e,err:=Parse(sipReq("INVITE",id,fmt.Sprintf("z9hG4bK-%d",i),"a","",1),Outbound,Meta{ObservedAt:at,CorrelationID:"app-call"},ParseOptions{});if err!=nil{t.Fatal(err)};tr.Apply(e)}
 f,ok:=tr.Flow("app-call");if !ok{t.Fatal("flow missing")};if len(f.CallIDs)!=2{t.Fatalf("CallIDs=%v",f.CallIDs)}
}

func TestTrackerForkCreatesMultipleDialogs(t *testing.T) {
 tr:=NewTracker(); at:=time.Now()
 req,_:=Parse(sipReq("INVITE","fork","z9hG4bK-f","a","",1),Outbound,Meta{ObservedAt:at},ParseOptions{});tr.Apply(req)
 for i,tag:=range []string{"b1","b2"} { e,_:=Parse(sipResp(180,"Ringing","fork","z9hG4bK-f","a",tag,"INVITE",1),Inbound,Meta{ObservedAt:at.Add(time.Duration(i+1)*time.Millisecond)},ParseOptions{});tr.Apply(e)}
 f,_:=tr.Flow("fork");if len(f.Dialogs)!=2{t.Fatalf("dialogs=%d",len(f.Dialogs))}
}

func TestTrackerCancel487(t *testing.T) {
 tr:=NewTracker();at:=time.Now()
 raws:=[][]byte{sipReq("INVITE","cancel","z9hG4bK-i","a","",1),sipReq("CANCEL","cancel","z9hG4bK-i","a","",1),sipResp(487,"Request Terminated","cancel","z9hG4bK-i","a","b","INVITE",1)}
 var f Flow
 for i,r:=range raws{e,err:=Parse(r,Inbound,Meta{ObservedAt:at.Add(time.Duration(i)*time.Millisecond)},ParseOptions{});if err!=nil{t.Fatal(err)};f,_=tr.Apply(e)}
 if !f.Cancelled||f.State!=StateCancelled{t.Fatalf("cancelled=%v state=%s",f.Cancelled,f.State)}
}

type blockingSink struct{ gate chan struct{}; entered chan struct{}; once sync.Once }
func(s *blockingSink)WriteEvent(context.Context,Event)error{s.once.Do(func(){close(s.entered)});<-s.gate;return nil}
func(s *blockingSink)CompleteFlow(context.Context,Flow)error{return nil}

func TestRecorderDropNewestBackpressure(t *testing.T) {
 s:=&blockingSink{gate:make(chan struct{}),entered:make(chan struct{})};r:=New(WithQueueSize(1),WithSink(s),WithOverflowPolicy(DropNewest))
 raw:=sipReq("OPTIONS","bp","z9hG4bK-bp","a","",1)
 if err:=r.Observe(context.Background(),Inbound,raw,Meta{});err!=nil{t.Fatal(err)}
 select{case<-s.entered:case<-time.After(time.Second):t.Fatal("worker did not enter sink")}
 if err:=r.Observe(context.Background(),Inbound,raw,Meta{});err!=nil{t.Fatalf("fill queue: %v",err)}
 if err:=r.Observe(context.Background(),Inbound,raw,Meta{});!errors.Is(err,ErrQueueFull){t.Fatalf("want queue full, got %v",err)}
 close(s.gate);ctx,cancel:=context.WithTimeout(context.Background(),time.Second);defer cancel();if err:=r.Close(ctx);err!=nil{t.Fatal(err)}
 if err:=r.Observe(context.Background(),Inbound,raw,Meta{});!errors.Is(err,ErrRecorderClosed){t.Fatalf("want closed, got %v",err)}
}

type failingSink struct{ calls atomic.Int32 }
func(s *failingSink)WriteEvent(context.Context,Event)error{s.calls.Add(1);return errors.New("sink failed")}
func(s *failingSink)CompleteFlow(context.Context,Flow)error{return errors.New("complete failed")}

func TestRecorderSinkFailureDoesNotStopWorker(t *testing.T) {
 s:=&failingSink{};errs:=make(chan error,4);r:=New(WithSink(s),WithErrorHandler(func(e error){errs<-e}))
 for i:=0;i<2;i++{if err:=r.Observe(context.Background(),Inbound,sipReq("OPTIONS",fmt.Sprintf("f-%d",i),fmt.Sprintf("z9hG4bK-%d",i),"a","",1),Meta{});err!=nil{t.Fatal(err)}}
 ctx,cancel:=context.WithTimeout(context.Background(),time.Second);defer cancel();if err:=r.Close(ctx);err!=nil{t.Fatal(err)}
 if s.calls.Load()!=2{t.Fatalf("sink calls=%d",s.calls.Load())}
 if len(errs)!=2{t.Fatalf("errors=%d",len(errs))}
}

func TestRecorderConcurrentObserveAndClose(t *testing.T) {
 r:=New(WithQueueSize(4096));var wg sync.WaitGroup
 for g:=0;g<16;g++{wg.Add(1);go func(g int){defer wg.Done();for i:=0;i<100;i++{_ = r.Observe(context.Background(),Inbound,sipReq("OPTIONS",fmt.Sprintf("c-%d-%d",g,i),fmt.Sprintf("z9hG4bK-%d-%d",g,i),"a","",1),Meta{})}}(g)}
 wg.Wait();ctx,cancel:=context.WithTimeout(context.Background(),3*time.Second);defer cancel();if err:=r.Close(ctx);err!=nil{t.Fatal(err)}
}
