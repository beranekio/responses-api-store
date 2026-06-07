package client

import (
	"encoding/json"
	"testing"
)

func TestRoundTripStoredResponse(t *testing.T) {
	enqueued := int64(1_746_500_000)
	record := StoredResponse{
		Upstream:               "http://model",
		Response:               json.RawMessage(`{"id":"resp_test","status":"queued"}`),
		Input:                  []json.RawMessage{json.RawMessage(`{"role":"user","content":"hi"}`)},
		PendingUpstreamRequest: json.RawMessage(`{"model":"demo","input":"hi"}`),
		UpstreamAuthorization:  "Bearer token",
		EnqueuedAt:             &enqueued,
	}

	protoRecord, err := toProtoRecord("resp_test", record)
	if err != nil {
		t.Fatalf("toProtoRecord: %v", err)
	}

	roundTripped, err := fromProtoRecord(protoRecord)
	if err != nil {
		t.Fatalf("fromProtoRecord: %v", err)
	}

	if roundTripped.Upstream != record.Upstream {
		t.Fatalf("upstream mismatch: %s", roundTripped.Upstream)
	}
	if string(roundTripped.Response) != string(record.Response) {
		t.Fatalf("response mismatch: %s", roundTripped.Response)
	}
	if len(roundTripped.Input) != 1 || string(roundTripped.Input[0]) != string(record.Input[0]) {
		t.Fatalf("input mismatch: %#v", roundTripped.Input)
	}
	if string(roundTripped.PendingUpstreamRequest) != string(record.PendingUpstreamRequest) {
		t.Fatalf("pending request mismatch: %s", roundTripped.PendingUpstreamRequest)
	}
	if roundTripped.UpstreamAuthorization != record.UpstreamAuthorization {
		t.Fatalf("authorization mismatch: %s", roundTripped.UpstreamAuthorization)
	}
	if roundTripped.EnqueuedAt == nil || *roundTripped.EnqueuedAt != enqueued {
		t.Fatalf("enqueued_at mismatch: %#v", roundTripped.EnqueuedAt)
	}
}