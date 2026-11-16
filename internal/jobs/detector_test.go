package jobs

import (
	"context"
	"encoding/json"
	"testing"

	"gamesense/internal/broker"
	"gamesense/internal/detect"
	"gamesense/internal/event"
)

func TestPoisonRecordGoesToDLQAndPipelineContinues(t *testing.T) {
	h := Detector(detect.New(detect.DefaultRules()), "detections", "telemetry.dlq")
	good, _ := json.Marshal([]event.Event{{MatchID: "m", PlayerID: "p", Type: event.TypeTick, TS: 1}})
	recs := []*broker.Record{
		{Topic: "telemetry.raw", Partition: 3, Offset: 41, Key: []byte("m0"), Value: []byte("this is not json")},
		{Topic: "telemetry.raw", Partition: 3, Offset: 42, Key: []byte("m"), Value: good},
	}
	out, err := h(context.Background(), recs)
	if err != nil {
		t.Fatalf("poison record must not fail the batch: %v", err)
	}
	if len(out) != 1 || out[0].Topic != "telemetry.dlq" {
		t.Fatalf("want exactly one DLQ record, got %+v", out)
	}
	var dl DeadLetter
	if err := json.Unmarshal(out[0].Value, &dl); err != nil {
		t.Fatal(err)
	}
	if dl.Offset != 41 || dl.Partition != 3 || string(dl.Value) != "this is not json" || dl.Reason != "decode" {
		t.Fatalf("dead letter lost context: %+v", dl)
	}
}
