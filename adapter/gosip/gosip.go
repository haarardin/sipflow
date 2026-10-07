package gosipadapter

import (
 "context"
 "strings"
 "github.com/haarardin/sipflow"
)

type Message interface { String() string; Transport() string; Source() string; Destination() string }
type Options struct { Node, CorrelationID, LegID string }
func Observe(ctx context.Context, r *sipflow.Recorder, direction sipflow.Direction, msg Message, o Options) error {
 m:=sipflow.Meta{Transport:sipflow.Transport(strings.ToUpper(msg.Transport())),Node:o.Node,CorrelationID:o.CorrelationID,LegID:o.LegID}
 if direction==sipflow.Inbound { m.RemoteAddr=msg.Source(); m.LocalAddr=msg.Destination() } else { m.LocalAddr=msg.Source(); m.RemoteAddr=msg.Destination() }
 return r.Observe(ctx,direction,[]byte(msg.String()),m)
}
