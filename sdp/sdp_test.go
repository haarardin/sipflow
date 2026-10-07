package sdp
import "testing"
func TestParseWebRTCSummary(t *testing.T){s:=Parse("v=0\r\nc=IN IP4 203.0.113.1\r\na=group:BUNDLE 0\r\na=ice-ufrag:abc\r\na=fingerprint:sha-256 11:22\r\nm=audio 49170 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\na=rtcp-mux\r\na=sendrecv\r\na=rtpmap:111 opus/48000/2\r\n");if s.ConnectionAddress!="203.0.113.1"||s.ICEUfrag!="abc"||len(s.Media)!=1{t.Fatalf("summary=%+v",s)};m:=s.Media[0];if !m.RTCPMux||m.MID!="0"||len(m.Codecs)!=1||m.Codecs[0].Name!="opus"||m.Codecs[0].ClockRate!=48000||m.Codecs[0].Channels!=2{t.Fatalf("media=%+v",m)}}
func FuzzParse(f *testing.F){f.Add("v=0\r\nm=audio 9 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\n");f.Add("");f.Fuzz(func(t *testing.T,s string){_ = Parse(s)})}
