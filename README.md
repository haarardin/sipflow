# sipflow

Embeddable SIP observability for Go.

`sipflow` turns copies of inbound and outbound SIP messages into structured call flows, transactions, dialogs, timing metrics and compact SDP summaries. It sits beside a SIP stack rather than in the signaling path, so observability failures do not need to break calls.

> Status: early v0.1 API.

## Goals

- library-first integration into real SIP services;
- RFC-aware call-flow evidence without replacing the SIP stack;
- bounded asynchronous observation with non-blocking defaults;
- B2BUA support through an application-level `CorrelationID`;
- structured events suitable for JSONL, storage, diagrams and debugging;
- no third-party dependencies in the core module.

## Quick start

```go
recorder := sipflow.New(
    sipflow.WithQueueSize(2048),
)
defer recorder.Close(context.Background())

err := recorder.Observe(ctx, sipflow.Inbound, rawSIP, sipflow.Meta{
    LocalAddr:  "10.0.0.10:5060",
    RemoteAddr: "10.0.0.20:5060",
    Transport:  sipflow.TransportUDP,
})
```

Raw SIP payloads are not retained by default. Enable `WithCaptureRaw(true)` only when appropriate for your privacy/security model.

## ghettovoice/gosip

`adapter/gosip` deliberately uses a structural interface matching the methods already exposed by `gosip/sip.Message`: `String()`, `Transport()`, `Source()` and `Destination()`. This lets a gosip Request/Response be passed directly without forcing a gosip version into this module.

```go
err := gosipadapter.Observe(ctx, recorder, sipflow.Inbound, request, gosipadapter.Options{
    Node:          "edge-sip-1",
    CorrelationID: appCallID,
    LegID:         "pstn-leg",
})
```

## Correlation

A flow is keyed by SIP `Call-ID` by default. For a B2BUA or gateway, `Meta.CorrelationID` joins several SIP legs with different Call-IDs into one application flow while preserving the real Call-ID on every event.

Transactions use an observability key composed from Call-ID, top Via branch, CSeq number and CSeq method. Dialogs are observed from Call-ID plus the two endpoint tags. This is intentionally not a replacement for RFC 3261 transaction/dialog machinery.

## Derived data

v0.1 currently records requests/responses, endpoints and transport, retransmission evidence, transactions, dialogs, INVITE provisional/ringing/answer timing, early-media observation (183), CANCEL/BYE state, and SDP media/codec/rtcp-mux/BUNDLE/ICE/fingerprint summaries.

The SDP code is a summary extractor, not a complete SDP validator or offer/answer engine.

## Backpressure

The recorder has a bounded queue. The default is `DropNewest`, because tracing should not unexpectedly block production signaling. `DropOldest` and `Block` are also available. Treat `ErrQueueFull` as an observability metric rather than a SIP signaling error.

## Protocol scope

SIP terminology and correlation are based primarily on RFC 3261. Future work should expand forked dialogs, CANCEL/487 races, re-INVITE/UPDATE, PRACK, authentication challenges and media negotiation history.

## Development

```bash
gofmt -w .
go vet ./...
go test -race ./...
```

## License

MIT
