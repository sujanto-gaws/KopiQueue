// Spike 0.8 observer: drives River through its *public* API and reports what lands in the database
// and on the wire (LISTEN/NOTIFY). Clean-room: observes behaviour only; River's source is not read.
//
// Usage: RIVER_SPIKE_URL=postgres://... go run . <insert|work>
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

type SpikeArgs struct {
	N        int    `json:"n"`
	Customer string `json:"customer"`
}

func (SpikeArgs) Kind() string { return "spike_args" }

// UniqueFieldArgs marks one field with River's documented `river:"unique"` tag.
type UniqueFieldArgs struct {
	CustomerID string `json:"customer_id" river:"unique"`
	Note       string `json:"note"`
}

func (UniqueFieldArgs) Kind() string { return "spike_unique_field" }

type SpikeWorker struct{ river.WorkerDefaults[SpikeArgs] }

func (w *SpikeWorker) Work(ctx context.Context, job *river.Job[SpikeArgs]) error { return nil }

type UniqueFieldWorker struct{ river.WorkerDefaults[UniqueFieldArgs] }

func (w *UniqueFieldWorker) Work(ctx context.Context, job *river.Job[UniqueFieldArgs]) error { return nil }

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func dumpJob(ctx context.Context, pool *pgxpool.Pool, label string, id int64, skipped bool) {
	var state, kind, queue string
	var args, metadata []byte
	var priority, attempt, maxAttempts int16
	var tags []string
	var uniqueKey []byte
	var uniqueStates *string
	var scheduledAt, createdAt time.Time
	must(pool.QueryRow(ctx, `SELECT state::text, kind, queue, args, metadata, priority, attempt, max_attempts, tags,
		unique_key, unique_states::text, scheduled_at, created_at FROM river_job WHERE id = $1`, id).
		Scan(&state, &kind, &queue, &args, &metadata, &priority, &attempt, &maxAttempts, &tags, &uniqueKey, &uniqueStates, &scheduledAt, &createdAt))
	us := "NULL"
	if uniqueStates != nil {
		us = *uniqueStates
	}
	fmt.Printf("- **%s:** id=%d skipped=%v state=%s kind=%s queue=%s priority=%d attempt=%d/%d tags=%v "+
		"scheduled_in=%s args=%s metadata=%s unique_key=%s (%d bytes) unique_states=%s\n",
		label, id, skipped, state, kind, queue, priority, attempt, maxAttempts, tags,
		scheduledAt.Sub(createdAt).Round(time.Minute), args, metadata, hex.EncodeToString(uniqueKey), len(uniqueKey), us)
}

func notifications(ctx context.Context, pool *pgxpool.Pool) string {
	rows, err := pool.Query(ctx, `SELECT topic, payload FROM river_notification ORDER BY id`)
	must(err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var t, p string
		must(rows.Scan(&t, &p))
		out = append(out, t+" "+p)
	}
	if len(out) == 0 {
		return "(none)"
	}
	return strings.Join(out, " | ")
}

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("RIVER_SPIKE_URL"))
	must(err)
	defer pool.Close()

	switch os.Args[1] {
	case "golden":
		runGolden(ctx, pool)
	case "insert":
		client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{})
		must(err)

		r, err := client.Insert(ctx, SpikeArgs{N: 1, Customer: "a"}, nil)
		must(err)
		dumpJob(ctx, pool, "plain insert (insert-only client)", r.Job.ID, r.UniqueSkippedAsDuplicate)
		fmt.Printf("- **river_notification after plain insert:** %s\n", notifications(ctx, pool))

		for i, opts := range []*river.InsertOpts{
			{UniqueOpts: river.UniqueOpts{ByArgs: true}},
			{UniqueOpts: river.UniqueOpts{ByArgs: true}},
			{UniqueOpts: river.UniqueOpts{ByQueue: true}},
			{UniqueOpts: river.UniqueOpts{ByPeriod: time.Hour}},
			{UniqueOpts: river.UniqueOpts{ByArgs: true, ByQueue: true, ByPeriod: time.Hour}},
			{UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRunning, rivertype.JobStateScheduled}}},
		} {
			r, err := client.Insert(ctx, SpikeArgs{N: 2, Customer: "b"}, opts)
			must(err)
			o, _ := json.Marshal(opts.UniqueOpts)
			dumpJob(ctx, pool, fmt.Sprintf("unique #%d %s", i+1, o), r.Job.ID, r.UniqueSkippedAsDuplicate)
		}

		r, err = client.Insert(ctx, UniqueFieldArgs{CustomerID: "c-1", Note: "first"}, &river.InsertOpts{UniqueOpts: river.UniqueOpts{ByArgs: true}})
		must(err)
		dumpJob(ctx, pool, "unique field-tag, note=first", r.Job.ID, r.UniqueSkippedAsDuplicate)
		r, err = client.Insert(ctx, UniqueFieldArgs{CustomerID: "c-1", Note: "second"}, &river.InsertOpts{UniqueOpts: river.UniqueOpts{ByArgs: true}})
		must(err)
		dumpJob(ctx, pool, "unique field-tag, note=second (same customer_id)", r.Job.ID, r.UniqueSkippedAsDuplicate)

		r, err = client.Insert(ctx, SpikeArgs{N: 3}, &river.InsertOpts{
			Queue: "other", Priority: 3, Tags: []string{"tag-one", "tag_two"}, MaxAttempts: 5,
			ScheduledAt: time.Now().Add(time.Hour), Metadata: []byte(`{"user":"meta"}`),
		})
		must(err)
		dumpJob(ctx, pool, "insert with options (scheduled +1h)", r.Job.ID, r.UniqueSkippedAsDuplicate)

		tx, err := pool.Begin(ctx)
		must(err)
		r, err = client.InsertTx(ctx, tx, SpikeArgs{N: 4}, nil)
		must(err)
		must(tx.Rollback(ctx))
		var n int
		must(pool.QueryRow(ctx, `SELECT count(*) FROM river_job WHERE id = $1`, r.Job.ID).Scan(&n))
		fmt.Printf("- **InsertTx then rollback:** rows remaining=%d\n", n)
		fmt.Printf("- **river_notification after all inserts:** %s\n", notifications(ctx, pool))

	case "work":
		workers := river.NewWorkers()
		river.AddWorker(workers, &SpikeWorker{})
		river.AddWorker(workers, &UniqueFieldWorker{})
		client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
			Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 2}, "other": {MaxWorkers: 1}},
			Workers: workers,
		})
		must(err)
		completed, cancel := client.Subscribe(river.EventKindJobCompleted)
		defer cancel()
		must(client.Start(ctx))
		time.Sleep(2 * time.Second)

		// Interface discovery only: which channels does a running client LISTEN on?
		rows, err := pool.Query(ctx, `SELECT DISTINCT query FROM pg_stat_activity
			WHERE datname = current_database() AND pid <> pg_backend_pid() AND query ILIKE 'listen %'`)
		must(err)
		var listens []string
		for rows.Next() {
			var q string
			must(rows.Scan(&q))
			listens = append(listens, strings.TrimSpace(q))
		}
		rows.Close()
		fmt.Printf("- **LISTEN statements visible in pg_stat_activity:** %v\n", listens)

		// Observe insert-time notifications from a separate connection on likely channels.
		obs, err := pgx.Connect(ctx, os.Getenv("RIVER_SPIKE_URL"))
		must(err)
		defer obs.Close(ctx)
		// Discovered channels plus schema-qualified and bare candidates for the other topics.
		channels := channelsFrom(append(listens, "LISTEN public.river_insert", "LISTEN public.river_leadership",
			"LISTEN public.river_control", "LISTEN river_insert", "LISTEN river_leadership", "LISTEN river_control"))
		fmt.Printf("- **observer LISTENs on:** %v\n", channels)
		for _, ch := range channels {
			_, err := obs.Exec(ctx, "LISTEN "+pgx.Identifier{ch}.Sanitize())
			must(err)
		}

		start := time.Now()
		r, err := client.Insert(ctx, SpikeArgs{N: 10, Customer: "live"}, nil)
		must(err)
		got := 0
		deadline := time.After(10 * time.Second)
	wait:
		for {
			select {
			case ev := <-completed:
				if ev.Job.ID == r.Job.ID {
					fmt.Printf("- **live insert → completed:** %s (state=%s attempt=%d attempted_by=%v)\n",
						time.Since(start).Round(time.Millisecond), ev.Job.State, ev.Job.Attempt, ev.Job.AttemptedBy)
					break wait
				}
			case <-deadline:
				fmt.Println("- **live insert:** not completed within 10 s")
				break wait
			}
		}
		nctx, ncancel := context.WithTimeout(ctx, 2*time.Second)
		for {
			n, err := obs.WaitForNotification(nctx)
			if err != nil {
				break
			}
			got++
			fmt.Printf("- **NOTIFY observed:** channel=%s payload=%s\n", n.Channel, n.Payload)
		}
		ncancel()
		fmt.Printf("- **notifications observed for one insert:** %d\n", got)
		time.Sleep(6 * time.Second) // let leadership and queue maintenance happen
		must(client.Stop(ctx))

		var leaders, queues string
		must(pool.QueryRow(ctx, `SELECT coalesce(string_agg(name || ' expires_in=' || round(extract(epoch from expires_at - elected_at))::text || 's', ', '), '(none)') FROM river_leader`).Scan(&leaders))
		must(pool.QueryRow(ctx, `SELECT coalesce(string_agg(name || ' paused=' || (paused_at IS NOT NULL)::text || ' metadata=' || metadata::text, ', ' ORDER BY name), '(none)') FROM river_queue`).Scan(&queues))
		fmt.Printf("- **river_leader after stop:** %s\n- **river_queue rows:** %s\n- **river_notification after work:** %s\n", leaders, queues, notifications(ctx, pool))
		var states string
		must(pool.QueryRow(ctx, `SELECT string_agg(state::text || '=' || c, ', ' ORDER BY state) FROM (SELECT state, count(*) c FROM river_job GROUP BY state) s`).Scan(&states))
		fmt.Printf("- **job states after work:** %s\n", states)
	}
}

// channelsFrom extracts channel names from LISTEN statements.
func channelsFrom(listens []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, l := range listens {
		f := strings.Fields(l)
		if len(f) >= 2 {
			ch := strings.Trim(strings.TrimSuffix(f[1], ";"), `"`)
			if !seen[ch] {
				seen[ch] = true
				out = append(out, ch)
			}
		}
	}
	return out
}
