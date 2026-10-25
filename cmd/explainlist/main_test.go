package main

import "testing"

// TestBufferRatio exercises parseBuffers and bufferRatio against a
// hand-built EXPLAIN (FORMAT JSON) plan tree: no database needed, since
// both functions are pure over already-fetched JSON.
func TestBufferRatio(t *testing.T) {
	wholePlan := []byte(`[{"Plan": {
		"Node Type": "Limit",
		"Shared Hit Blocks": 10,
		"Shared Read Blocks": 90,
		"Plans": [{
			"Node Type": "Index Scan",
			"Shared Hit Blocks": 5,
			"Shared Read Blocks": 895
		}]
	}}]`)
	askedPlan := []byte(`[{"Plan": {
		"Node Type": "Limit",
		"Shared Hit Blocks": 8,
		"Shared Read Blocks": 2,
		"Plans": [{
			"Node Type": "Index Scan",
			"Shared Hit Blocks": 40,
			"Shared Read Blocks": 0
		}]
	}}]`)

	whole, err := parseBuffers(wholePlan)
	if err != nil {
		t.Fatal(err)
	}
	if whole.hit != 15 || whole.read != 985 || whole.total() != 1000 {
		t.Fatalf("whole buffers: %+v", whole)
	}

	asked, err := parseBuffers(askedPlan)
	if err != nil {
		t.Fatal(err)
	}
	if asked.hit != 48 || asked.read != 2 || asked.total() != 50 {
		t.Fatalf("asked buffers: %+v", asked)
	}

	if got, want := bufferRatio(asked, whole), "0.0500"; got != want {
		t.Fatalf("ratio = %s, want %s", got, want)
	}
	if got, want := bufferRatio(whole, whole), "1.0000"; got != want {
		t.Fatalf("self ratio = %s, want %s", got, want)
	}
	if got, want := bufferRatio(asked, buffers{}), "n/a"; got != want {
		t.Fatalf("ratio against zero whole = %s, want %s", got, want)
	}

	if _, err := parseBuffers([]byte(`[]`)); err == nil {
		t.Fatal("expected error for empty plan list")
	}
}
