// kopi-river-compat drives a real River client (v0.51.2) through its *public* API, so the KopiQueue interop tests
// (plan task 2.10, NFR-COMP-05) can run River and KopiQueue on one schema and compare what each writes.
//
// Clean-room (CONTRIBUTING.md, ADR-008): River is used only as a black box. Its source is never read; behaviour is
// learnt by running it and inspecting the database.
//
// The connection comes from KOPI_COMPAT_PG (a libpq keyword/value string or a postgres:// URL), never from the command
// line. Every event is one JSON object per line on stdout.
//
//	kopi-river-compat insert -schema S -queue Q -what ok -count 3 [-max-attempts N] [-many]
//	kopi-river-compat work   -schema S -queue Q -expect 3 [-poll 1s] [-timeout 30s] [-id river-compat] [-min-run 0s]
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// CompatArgs is shared with the C# tests (kind compat_task, args {"what": ...}).
type CompatArgs struct {
	What string `json:"what"`
}

func (CompatArgs) Kind() string { return "compat_task" }

// CompatWorker does what each job's args say, the same scripts as the C# side's worker.
type CompatWorker struct {
	river.WorkerDefaults[CompatArgs]
	out *output
}

// NextRetry puts failed jobs ten minutes out on both sides, so the stored rows compare without timing races.
func (w *CompatWorker) NextRetry(job *river.Job[CompatArgs]) time.Time {
	return time.Now().Add(10 * time.Minute)
}

func (w *CompatWorker) Work(ctx context.Context, job *river.Job[CompatArgs]) error {
	defer w.out.emit(map[string]any{"event": "worked", "id": job.ID, "what": job.Args.What, "attempt": job.Attempt})
	switch job.Args.What {
	case "ok":
		return nil
	case "fail", "fail-last":
		return errors.New("compat failure")
	case "snooze":
		return river.JobSnooze(10 * time.Minute)
	case "cancel":
		return river.JobCancel(errors.New("compat cancel"))
	default:
		return fmt.Errorf("unknown script %q", job.Args.What)
	}
}

type output struct {
	mu     sync.Mutex
	enc    *json.Encoder
	worked int
	cond   chan struct{}
}

func (o *output) emit(event map[string]any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if event["event"] == "worked" {
		o.worked++
		select {
		case o.cond <- struct{}{}:
		default:
		}
	}
	_ = o.enc.Encode(event)
}

func (o *output) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.worked
}

func fail(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func main() {
	if len(os.Args) < 2 {
		fail(errors.New("usage: kopi-river-compat <insert|work> [flags]"))
	}

	flags := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	schema := flags.String("schema", "", "schema holding the River tables")
	queue := flags.String("queue", "default", "queue to insert into or work")
	what := flags.String("what", "ok", "script for inserted jobs: ok, fail, fail-last, snooze, cancel")
	count := flags.Int("count", 1, "jobs to insert")
	maxAttempts := flags.Int("max-attempts", 0, "max attempts of inserted jobs (0: River's default)")
	many := flags.Bool("many", false, "insert all jobs with one InsertMany call (one transaction) instead of one Insert each")
	expect := flags.Int("expect", 1, "jobs to work before stopping")
	poll := flags.Duration("poll", time.Second, "River's FetchPollInterval")
	timeout := flags.Duration("timeout", 30*time.Second, "give up after this long")
	id := flags.String("id", "river-compat", "River client ID")
	minRun := flags.Duration("min-run", 0, "keep the worker running at least this long (to observe leadership)")
	fail(flags.Parse(os.Args[2:]))

	dsn := os.Getenv("KOPI_COMPAT_PG")
	if dsn == "" {
		fail(errors.New("set KOPI_COMPAT_PG"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	fail(err)
	defer pool.Close()

	out := &output{enc: json.NewEncoder(os.Stdout), cond: make(chan struct{}, 1)}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	switch os.Args[1] {
	case "insert":
		// An insert-only client: no queues, no workers.
		client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{Schema: *schema, Logger: logger})
		fail(err)
		opts := &river.InsertOpts{Queue: *queue}
		if *maxAttempts > 0 {
			opts.MaxAttempts = *maxAttempts
		}
		if *many {
			params := make([]river.InsertManyParams, *count)
			for i := range params {
				params[i] = river.InsertManyParams{Args: CompatArgs{What: *what}, InsertOpts: opts}
			}
			results, err := client.InsertMany(ctx, params)
			fail(err)
			for _, res := range results {
				out.emit(map[string]any{"event": "inserted", "id": res.Job.ID})
			}
		} else {
			for i := 0; i < *count; i++ {
				res, err := client.Insert(ctx, CompatArgs{What: *what}, opts)
				fail(err)
				out.emit(map[string]any{"event": "inserted", "id": res.Job.ID})
			}
		}
		out.emit(map[string]any{"event": "done"})

	case "work":
		workers := river.NewWorkers()
		river.AddWorker(workers, &CompatWorker{out: out})
		client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
			ID:                *id,
			Schema:            *schema,
			Queues:            map[string]river.QueueConfig{*queue: {MaxWorkers: 5}},
			Workers:           workers,
			FetchPollInterval: *poll,
			Logger:            logger,
		})
		fail(err)
		fail(client.Start(ctx))
		started := time.Now()
		out.emit(map[string]any{"event": "ready"})
		for out.count() < *expect {
			select {
			case <-out.cond:
			case <-ctx.Done():
				out.emit(map[string]any{"event": "timeout", "worked": out.count()})
				os.Exit(2)
			}
		}

		if rest := *minRun - time.Since(started); rest > 0 {
			time.Sleep(rest)
		}

		stop, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer stopCancel()
		fail(client.Stop(stop)) // waits for running jobs and writes their outcomes
		out.emit(map[string]any{"event": "done", "worked": out.count()})

	default:
		fail(fmt.Errorf("unknown command %q", os.Args[1]))
	}
}
