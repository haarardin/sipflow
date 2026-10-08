package sipflow
import "testing"
func FuzzParse(f *testing.F){f.Add([]byte("OPTIONS sip:b@example.com SIP/2.0\r\nVia: SIP/2.0/UDP a;branch=z9hG4bK-a\r\nFrom: <sip:a@example.com>;tag=a\r\nTo: <sip:b@example.com>\r\nCall-ID: fuzz\r\nCSeq: 1 OPTIONS\r\n\r\n"));f.Add([]byte("garbage"));f.Fuzz(func(t *testing.T,b []byte){_,_ = Parse(b,Inbound,Meta{},ParseOptions{})})}
