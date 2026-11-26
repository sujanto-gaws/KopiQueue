package main

// Golden-vector generator: River computes the unique_key / unique_states for a corpus of inputs,
// and we record River's *outputs*. KopiQueue's M2 conformance tests must reproduce them byte for byte.

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

type MapArgs map[string]any // only usable without ByArgs

func (MapArgs) Kind() string { return "golden_map" }

// GenArgs wraps arbitrary JSON so nested-ordering/escaping rules can be probed with ByArgs (struct required).
type GenArgs struct {
	V string `json:"v"`
	P any    `json:"p,omitempty"`
}

func (GenArgs) Kind() string { return "golden_gen" }

type Inner struct {
	B string `json:"b"`
	A int    `json:"a"`
}

type TaggedArgs struct {
	Region   string `json:"region" river:"unique"`
	Customer Inner  `json:"customer" river:"unique"`
	Note     string `json:"note"`
	Skipped  string `json:"-"`
	Omit     string `json:"omit,omitempty" river:"unique"`
}

func (TaggedArgs) Kind() string { return "golden_tagged" }

type NoTagArgs struct {
	Z string `json:"z"`
	A string `json:"a"`
}

func (NoTagArgs) Kind() string { return "golden_notag" }

type NestedArgs struct {
	Z     string  `json:"z"`
	Inner Inner   `json:"inner"`
	List  []Inner `json:"list"`
}

func (NestedArgs) Kind() string { return "golden_nested" }

type vector struct {
	Name         string         `json:"name"`
	Kind         string         `json:"kind"`
	ArgsJSON     string         `json:"args_json"` // what json.Marshal produced for the args (input to KQ)
	Opts         map[string]any `json:"unique_opts"`
	Queue        string         `json:"queue"`
	CreatedAtUTC string         `json:"created_at_utc"`
	UniqueKeyHex string         `json:"unique_key_hex"`
	UniqueStates string         `json:"unique_states"`
}

func runGolden(ctx context.Context, pool *pgxpool.Pool) {
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{})
	must(err)
	allStates := []rivertype.JobState{
		rivertype.JobStateAvailable, rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded,
		rivertype.JobStatePending, rivertype.JobStateRetryable, rivertype.JobStateRunning, rivertype.JobStateScheduled,
	}
	type c struct {
		name  string
		args  river.JobArgs
		queue string
		u     river.UniqueOpts
	}
	byArgs := river.UniqueOpts{ByArgs: true}
	cases := []c{
		{"empty map", GenArgs{V: "empty map", P: map[string]any{}}, "", byArgs},
		{"key order", GenArgs{V: "key order", P: map[string]any{"zeta": 1, "alpha": 2, "Mid": 3, "_u": 4}}, "", byArgs},
		{"nested and arrays", GenArgs{V: "nested and arrays", P: map[string]any{"o": map[string]any{"y": []any{3, 1, map[string]any{"q": 1, "p": 2}}, "x": nil}}}, "", byArgs},
		{"html chars", GenArgs{V: "html chars", P: map[string]any{"s": "<a href='x'>&amp;</a>"}}, "", byArgs},
		{"unicode", GenArgs{V: "unicode", P: map[string]any{"s": "kopi ☕ café   \U0001F600", "e": "tab\tnl\nquote\"bs\\"}}, "", byArgs},
		{"numbers", GenArgs{V: "numbers", P: map[string]any{"i": 42, "neg": -7, "f": 1.5, "big": 9007199254740993, "exp": 1e21, "small": 1e-7, "zero": 0.0}}, "", byArgs},
		{"bools and null", GenArgs{V: "bools and null", P: map[string]any{"t": true, "f": false, "n": nil}}, "", byArgs},
		{"struct field order", NoTagArgs{Z: "last", A: "first"}, "", byArgs},
		{"untagged nested struct order", NestedArgs{Z: "n", Inner: Inner{B: "b", A: 2}, List: []Inner{{B: "y", A: 1}}}, "", byArgs},
		{"field tags", TaggedArgs{Region: "eu", Customer: Inner{B: "x", A: 1}, Note: "ignored", Skipped: "s"}, "", byArgs},
		{"field tags, other note", TaggedArgs{Region: "eu", Customer: Inner{B: "x", A: 1}, Note: "different"}, "q2", byArgs},
		{"field tags with omitempty set", TaggedArgs{Region: "us", Omit: "present"}, "", byArgs},
		{"by queue only", MapArgs{"v": 1}, "q_named", river.UniqueOpts{ByQueue: true}},
		{"kind only (no dimensions)", NoTagArgs{Z: "k"}, "", river.UniqueOpts{ByState: allStates}},
		{"gen with nil payload", GenArgs{V: "nilp"}, "", byArgs},
		{"by period 1h, map args", MapArgs{"v": 1}, "", river.UniqueOpts{ByPeriod: time.Hour}},
		{"exclude kind + args", GenArgs{V: "exclude kind + args", P: map[string]any{"v": "ek"}}, "", river.UniqueOpts{ByArgs: true, ExcludeKind: true}},
		{"by period 15m", MapArgs{"v": "p"}, "", river.UniqueOpts{ByPeriod: 15 * time.Minute}},
		{"by period 24h", MapArgs{"v": "p"}, "", river.UniqueOpts{ByPeriod: 24 * time.Hour}},
		{"all dims", GenArgs{V: "all dims", P: map[string]any{"v": "all"}}, "q_all", river.UniqueOpts{ByArgs: true, ByQueue: true, ByPeriod: time.Hour}},
		{"states: all 8", GenArgs{V: "states: all 8", P: map[string]any{"v": "s8"}}, "", river.UniqueOpts{ByArgs: true, ByState: allStates}},
		{"states: available only", GenArgs{V: "states: available only", P: map[string]any{"v": "s1"}}, "", river.UniqueOpts{ByArgs: true, ByState: allStates[:1]}},
		{"states: required minimum", GenArgs{V: "states: required minimum", P: map[string]any{"v": "smin"}}, "", river.UniqueOpts{ByArgs: true, ByState: []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRunning, rivertype.JobStateScheduled}}},
	}
	out, err := os.Create(os.Args[2])
	must(err)
	defer out.Close()
	enc := json.NewEncoder(out)
	for _, tc := range cases {
		argsJSON, err := json.Marshal(tc.args)
		must(err)
		opts := &river.InsertOpts{Queue: tc.queue, UniqueOpts: tc.u}
		r, err := client.Insert(ctx, tc.args, opts)
		if err != nil {
			fmt.Printf("- %-32s REJECTED: %v\n", tc.name, err)
			continue
		}
		var key []byte
		var states *string
		var created time.Time
		must(pool.QueryRow(ctx, `SELECT unique_key, unique_states::text, created_at FROM river_job WHERE id=$1`, r.Job.ID).Scan(&key, &states, &created))
		st := ""
		if states != nil {
			st = *states
		}
		var o map[string]any
		b, _ := json.Marshal(tc.u)
		must(json.Unmarshal(b, &o))
		v := vector{tc.name, tc.args.Kind(), string(argsJSON), o, r.Job.Queue, created.UTC().Format(time.RFC3339Nano), hex.EncodeToString(key), st}
		must(enc.Encode(v))
		fmt.Printf("- %-32s skipped=%v key=%s states=%s\n", tc.name, r.UniqueSkippedAsDuplicate, v.UniqueKeyHex[:16], st)
	}
}
