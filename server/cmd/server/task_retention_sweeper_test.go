package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	obsmetrics "github.com/multica-ai/multica/server/internal/metrics"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// TestParseTaskRetentionConfig is a pure unit test (no DB): defaults, valid
// overrides, and fail-closed rejection of every invalid destructive knob.
func TestParseTaskRetentionConfig(t *testing.T) {
	t.Parallel()

	base := func(kv map[string]string) func(string) string {
		return func(k string) string { return kv[k] }
	}

	t.Run("defaults when unset", func(t *testing.T) {
		cfg, err := parseTaskRetentionConfig(base(nil))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.FailedDays != taskRetentionDefaultFailedDays ||
			cfg.CompletedDays != taskRetentionDefaultCompletedDays ||
			cfg.Interval != taskRetentionDefaultInterval ||
			cfg.BatchSize != taskRetentionDefaultBatchSize ||
			!cfg.DryRun {
			t.Fatalf("defaults not applied: %+v", cfg)
		}
	})

	t.Run("valid overrides", func(t *testing.T) {
		cfg, err := parseTaskRetentionConfig(base(map[string]string{
			envTaskRetentionFailedDays:    "10",
			envTaskRetentionCompletedDays: "45",
			envTaskRetentionInterval:      "6h",
			envTaskRetentionBatchSize:     "250",
			envTaskRetentionDryRun:        "false",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.FailedDays != 10 || cfg.CompletedDays != 45 || cfg.Interval != 6*time.Hour ||
			cfg.BatchSize != 250 || cfg.DryRun {
			t.Fatalf("overrides not applied: %+v", cfg)
		}
	})

	t.Run("batch size at the hard cap is accepted", func(t *testing.T) {
		cfg, err := parseTaskRetentionConfig(base(map[string]string{
			envTaskRetentionBatchSize: "500",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.BatchSize != taskRetentionMaxBatchSize {
			t.Fatalf("batch size = %d, want %d", cfg.BatchSize, taskRetentionMaxBatchSize)
		}
	})

	invalid := []struct {
		name string
		env  map[string]string
	}{
		{"failed days non-integer", map[string]string{envTaskRetentionFailedDays: "abc"}},
		{"failed days zero", map[string]string{envTaskRetentionFailedDays: "0"}},
		{"failed days negative", map[string]string{envTaskRetentionFailedDays: "-1"}},
		{"completed days non-integer", map[string]string{envTaskRetentionCompletedDays: "x"}},
		{"completed days zero", map[string]string{envTaskRetentionCompletedDays: "0"}},
		{"interval unparseable", map[string]string{envTaskRetentionInterval: "soon"}},
		{"interval non-positive", map[string]string{envTaskRetentionInterval: "0s"}},
		{"batch size zero", map[string]string{envTaskRetentionBatchSize: "0"}},
		{"batch size over cap", map[string]string{envTaskRetentionBatchSize: "501"}},
		{"batch size non-integer", map[string]string{envTaskRetentionBatchSize: "lots"}},
		{"dry run non-boolean", map[string]string{envTaskRetentionDryRun: "maybe"}},
	}
	for _, tc := range invalid {
		t.Run("fail-closed: "+tc.name, func(t *testing.T) {
			if _, err := parseTaskRetentionConfig(base(tc.env)); err == nil {
				t.Fatalf("expected error for %v, got nil", tc.env)
			}
		})
	}
}

// --- DB-backed integration matrix ------------------------------------------
//
// These run against the PostgreSQL instance the package's TestMain connects to.
// When no database is reachable, TestMain prints "Skipping integration tests"
// and leaves testPool nil, so each test skips rather than passing vacuously.

const retentionFarPastDays = 3650 // 10 years: guarantees a candidate sorts oldest.

// retentionEnv holds a committed agent/autopilot the transactional cases
// reference. The tasks themselves are inserted inside a rolled-back
// transaction so the shared database is never mutated.
type retentionEnv struct {
	fx          *testutil.Fixture
	agentID     string
	autopilotID string
}

func newRetentionEnv(t *testing.T) *retentionEnv {
	t.Helper()
	fx := testutil.New(testPool, testWorkspaceID, testUserID)
	agentID := fx.Agent(t, "task-retention-agent", "")
	autopilotID := fx.Insert(t, "autopilot", testutil.Cols{
		"workspace_id":    testWorkspaceID,
		"title":           "task-retention-autopilot",
		"assignee_id":     agentID,
		"created_by_type": "member",
		"created_by_id":   testUserID,
	})
	return &retentionEnv{fx: fx, agentID: agentID, autopilotID: autopilotID}
}

// withRetentionTx runs fn inside a transaction that is always rolled back, so
// candidate rows the test inserts (and any it deletes) never persist. asOf is
// read from the DB clock inside the same transaction and iso is its literal
// form for building age-relative timestamps that use the exact SQL arithmetic
// the retention query uses.
func (e *retentionEnv) withRetentionTx(t *testing.T, fn func(ctx context.Context, tx pgx.Tx, qtx *db.Queries, asOf pgtype.Timestamptz, iso string)) {
	t.Helper()
	ctx := context.Background()
	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	qtx := db.New(tx)
	asOf, err := qtx.GetTaskRetentionAsOf(ctx)
	if err != nil {
		t.Fatalf("read DB clock: %v", err)
	}
	iso := asOf.Time.UTC().Format(time.RFC3339Nano)
	fn(ctx, tx, qtx, asOf, iso)
}

func execID(t *testing.T, ctx context.Context, tx pgx.Tx, sql string, args ...any) string {
	t.Helper()
	var id string
	if err := tx.QueryRow(ctx, sql, args...).Scan(&id); err != nil {
		t.Fatalf("insert: %v\nSQL: %s", err, sql)
	}
	return id
}

// insertTask inserts one agent_task_queue row with completed_at set to the
// given SQL expression (or NULL) and returns its id.
func (e *retentionEnv) insertTask(t *testing.T, ctx context.Context, tx pgx.Tx, status, completedAtExpr string) string {
	t.Helper()
	sql := fmt.Sprintf(
		"INSERT INTO agent_task_queue (agent_id, status, priority, completed_at) VALUES ($1, $2, 0, %s) RETURNING id",
		completedAtExpr)
	return execID(t, ctx, tx, sql, e.agentID, status)
}

// agedExpr builds a completed_at expression exactly `days` before asOf using
// the same make_interval arithmetic the retention predicate uses.
func agedExpr(iso string, days int) string {
	return fmt.Sprintf("'%s'::timestamptz - make_interval(days => %d)", iso, days)
}

func deletedSet(t *testing.T, ids []pgtype.UUID) map[string]bool {
	t.Helper()
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[util.UUIDToString(id)] = true
	}
	return set
}

func requireDBTest(t *testing.T) {
	if testPool == nil {
		t.Skip("integration tests skipped: no database")
	}
}

// TestTaskRetentionAgeBoundary proves the strict `<` age predicate: only rows
// strictly older than the threshold are candidates; the exact boundary, a newer
// row, and a NULL completed_at all survive. Covers both failed and completed.
func TestTaskRetentionAgeBoundary(t *testing.T) {
	requireDBTest(t)
	env := newRetentionEnv(t)

	env.withRetentionTx(t, func(ctx context.Context, tx pgx.Tx, qtx *db.Queries, asOf pgtype.Timestamptz, iso string) {
		// Failed stage, threshold 30 days.
		past := env.insertTask(t, ctx, tx, "failed", agedExpr(iso, retentionFarPastDays))
		exact := env.insertTask(t, ctx, tx, "failed", agedExpr(iso, 30))
		newer := env.insertTask(t, ctx, tx, "failed", agedExpr(iso, 29))
		nullCompleted := env.insertTask(t, ctx, tx, "failed", "NULL")

		ids, err := qtx.DeleteFailedTaskRetentionBatch(ctx, db.DeleteFailedTaskRetentionBatchParams{
			AsOf: asOf, FailedDays: 30, BatchSize: taskRetentionMaxBatchSize,
		})
		if err != nil {
			t.Fatalf("delete failed batch: %v", err)
		}
		got := deletedSet(t, ids)
		if !got[past] {
			t.Errorf("failed row past threshold was not deleted")
		}
		for name, id := range map[string]string{"exact-30d": exact, "newer-29d": newer, "null-completed": nullCompleted} {
			if got[id] {
				t.Errorf("failed row %s should have survived but was deleted", name)
			}
		}

		// Completed stage, threshold 90 days.
		cPast := env.insertTask(t, ctx, tx, "completed", agedExpr(iso, retentionFarPastDays))
		cExact := env.insertTask(t, ctx, tx, "completed", agedExpr(iso, 90))
		cNewer := env.insertTask(t, ctx, tx, "completed", agedExpr(iso, 89))

		cids, err := qtx.DeleteCompletedTaskRetentionBatch(ctx, db.DeleteCompletedTaskRetentionBatchParams{
			AsOf: asOf, CompletedDays: 90, BatchSize: taskRetentionMaxBatchSize,
		})
		if err != nil {
			t.Fatalf("delete completed batch: %v", err)
		}
		cgot := deletedSet(t, cids)
		if !cgot[cPast] {
			t.Errorf("completed row past threshold was not deleted")
		}
		if cgot[cExact] {
			t.Errorf("completed row at exact 90d boundary should have survived")
		}
		if cgot[cNewer] {
			t.Errorf("completed row newer than 90d should have survived")
		}
	})
}

// TestTaskRetentionOnlyTerminalStatuses proves the status-specific predicates:
// the failed sweep touches only failed rows and the completed sweep only
// completed rows; queued/running/cancelled/dispatched all survive regardless of
// age.
func TestTaskRetentionOnlyTerminalStatuses(t *testing.T) {
	requireDBTest(t)
	env := newRetentionEnv(t)

	env.withRetentionTx(t, func(ctx context.Context, tx pgx.Tx, qtx *db.Queries, asOf pgtype.Timestamptz, iso string) {
		old := agedExpr(iso, retentionFarPastDays)
		failed := env.insertTask(t, ctx, tx, "failed", old)
		completed := env.insertTask(t, ctx, tx, "completed", old)
		survivors := map[string]string{
			"queued":     env.insertTask(t, ctx, tx, "queued", old),
			"running":    env.insertTask(t, ctx, tx, "running", old),
			"cancelled":  env.insertTask(t, ctx, tx, "cancelled", old),
			"dispatched": env.insertTask(t, ctx, tx, "dispatched", old),
		}

		fids, err := qtx.DeleteFailedTaskRetentionBatch(ctx, db.DeleteFailedTaskRetentionBatchParams{
			AsOf: asOf, FailedDays: 30, BatchSize: taskRetentionMaxBatchSize,
		})
		if err != nil {
			t.Fatalf("delete failed batch: %v", err)
		}
		cids, err := qtx.DeleteCompletedTaskRetentionBatch(ctx, db.DeleteCompletedTaskRetentionBatchParams{
			AsOf: asOf, CompletedDays: 90, BatchSize: taskRetentionMaxBatchSize,
		})
		if err != nil {
			t.Fatalf("delete completed batch: %v", err)
		}

		fgot := deletedSet(t, fids)
		cgot := deletedSet(t, cids)
		if !fgot[failed] {
			t.Errorf("failed row was not deleted by the failed sweep")
		}
		if fgot[completed] {
			t.Errorf("completed row must not be deleted by the failed sweep")
		}
		if !cgot[completed] {
			t.Errorf("completed row was not deleted by the completed sweep")
		}
		if cgot[failed] {
			t.Errorf("failed row must not be deleted by the completed sweep")
		}
		for status, id := range survivors {
			if fgot[id] || cgot[id] {
				t.Errorf("non-terminal status %q must never be deleted", status)
			}
		}
	})
}

// insertRun inserts an autopilot_run linked to the environment's autopilot,
// with the given status/created_at, optionally pointing at taskID (direct
// guard). Returns the run id.
func (e *retentionEnv) insertRun(t *testing.T, ctx context.Context, tx pgx.Tx, status, createdAtExpr string, taskID *string) string {
	t.Helper()
	var task any
	if taskID != nil {
		task = *taskID
	}
	sql := fmt.Sprintf(
		"INSERT INTO autopilot_run (autopilot_id, source, status, created_at, task_id) VALUES ($1, 'manual', $2, %s, $3) RETURNING id",
		createdAtExpr)
	return execID(t, ctx, tx, sql, e.autopilotID, status, task)
}

// TestTaskRetentionAutopilotDirectGuard proves the direct guard
// (autopilot_run.task_id = task.id): an active run protects its aged task
// regardless of age; a terminal run inside the 7-day window (and exactly at it)
// protects; a terminal run older than 7 days does not.
func TestTaskRetentionAutopilotDirectGuard(t *testing.T) {
	requireDBTest(t)
	env := newRetentionEnv(t)

	env.withRetentionTx(t, func(ctx context.Context, tx pgx.Tx, qtx *db.Queries, asOf pgtype.Timestamptz, iso string) {
		old := agedExpr(iso, retentionFarPastDays)
		recency := func(days int) string {
			return fmt.Sprintf("'%s'::timestamptz - interval '%d days'", iso, days)
		}

		activeOld := env.insertTask(t, ctx, tx, "failed", old)
		env.insertRun(t, ctx, tx, "running", old, &activeOld) // active, ancient run still protects

		termRecent := env.insertTask(t, ctx, tx, "failed", old)
		env.insertRun(t, ctx, tx, "completed", recency(3), &termRecent)

		termExactly7 := env.insertTask(t, ctx, tx, "failed", old)
		env.insertRun(t, ctx, tx, "completed", recency(7), &termExactly7)

		termOld := env.insertTask(t, ctx, tx, "failed", old)
		env.insertRun(t, ctx, tx, "completed", recency(8), &termOld)

		ids, err := qtx.DeleteFailedTaskRetentionBatch(ctx, db.DeleteFailedTaskRetentionBatchParams{
			AsOf: asOf, FailedDays: 30, BatchSize: taskRetentionMaxBatchSize,
		})
		if err != nil {
			t.Fatalf("delete failed batch: %v", err)
		}
		got := deletedSet(t, ids)
		protected := map[string]string{
			"active-ancient-run": activeOld,
			"terminal-run-3d":    termRecent,
			"terminal-run-7d":    termExactly7,
		}
		for name, id := range protected {
			if got[id] {
				t.Errorf("direct guard: task with %s should be protected but was deleted", name)
			}
		}
		if !got[termOld] {
			t.Errorf("direct guard: task with terminal run older than 7d should be deletable")
		}
	})
}

// TestTaskRetentionAutopilotReverseGuard proves the reverse guard
// (autopilot_run.id = task.autopilot_run_id), which also covers retry lineage:
// a task pointing at an active run is protected; at a recent terminal run,
// protected; at an old terminal run, deletable.
func TestTaskRetentionAutopilotReverseGuard(t *testing.T) {
	requireDBTest(t)
	env := newRetentionEnv(t)

	env.withRetentionTx(t, func(ctx context.Context, tx pgx.Tx, qtx *db.Queries, asOf pgtype.Timestamptz, iso string) {
		old := agedExpr(iso, retentionFarPastDays)
		recency := func(days int) string {
			return fmt.Sprintf("'%s'::timestamptz - interval '%d days'", iso, days)
		}

		insertTaskWithRun := func(runID string) string {
			return execID(t, ctx, tx,
				fmt.Sprintf("INSERT INTO agent_task_queue (agent_id, status, priority, completed_at, autopilot_run_id) VALUES ($1, 'failed', 0, %s, $2) RETURNING id", old),
				env.agentID, runID)
		}

		activeRun := env.insertRun(t, ctx, tx, "running", old, nil)
		protectedByActive := insertTaskWithRun(activeRun)

		recentRun := env.insertRun(t, ctx, tx, "completed", recency(3), nil)
		protectedByRecent := insertTaskWithRun(recentRun)

		oldRun := env.insertRun(t, ctx, tx, "completed", recency(30), nil)
		deletable := insertTaskWithRun(oldRun)

		ids, err := qtx.DeleteFailedTaskRetentionBatch(ctx, db.DeleteFailedTaskRetentionBatchParams{
			AsOf: asOf, FailedDays: 30, BatchSize: taskRetentionMaxBatchSize,
		})
		if err != nil {
			t.Fatalf("delete failed batch: %v", err)
		}
		got := deletedSet(t, ids)
		if got[protectedByActive] {
			t.Errorf("reverse guard: task on active run should be protected")
		}
		if got[protectedByRecent] {
			t.Errorf("reverse guard: task on recent terminal run should be protected")
		}
		if !got[deletable] {
			t.Errorf("reverse guard: task on old terminal run should be deletable")
		}
	})
}

// TestTaskRetentionChildLineageGuard proves a task with a non-terminal child
// (retry lineage) is protected, while the same task becomes deletable once the
// child reaches a terminal status.
func TestTaskRetentionChildLineageGuard(t *testing.T) {
	requireDBTest(t)
	env := newRetentionEnv(t)

	env.withRetentionTx(t, func(ctx context.Context, tx pgx.Tx, qtx *db.Queries, asOf pgtype.Timestamptz, iso string) {
		old := agedExpr(iso, retentionFarPastDays)

		withChild := func(childStatus string) string {
			parent := env.insertTask(t, ctx, tx, "failed", old)
			execID(t, ctx, tx,
				"INSERT INTO agent_task_queue (agent_id, status, priority, parent_task_id) VALUES ($1, $2, 0, $3) RETURNING id",
				env.agentID, childStatus, parent)
			return parent
		}

		protectedParent := withChild("running")
		deletableParent := withChild("completed")

		// A three-generation chain pins the deliberate single-hop scope of the
		// guard. Retry lineage is single-hop: CreateRetryTask sets
		// parent_task_id to the immediate parent, and every resume/eligibility
		// query joins only that immediate parent (agent.sql:1476, 1522, 2198).
		// So a grandparent whose only direct child is terminal is deletable even
		// while a grandchild is still running — nothing ever reads past the
		// immediate parent, and that parent stays protected by its own
		// non-terminal child.
		grandparent := env.insertTask(t, ctx, tx, "failed", old)
		midParent := execID(t, ctx, tx,
			"INSERT INTO agent_task_queue (agent_id, status, priority, completed_at, parent_task_id) VALUES ($1, 'failed', 0, "+old+", $2) RETURNING id",
			env.agentID, grandparent)
		execID(t, ctx, tx,
			"INSERT INTO agent_task_queue (agent_id, status, priority, parent_task_id) VALUES ($1, 'running', 0, $2) RETURNING id",
			env.agentID, midParent)

		ids, err := qtx.DeleteFailedTaskRetentionBatch(ctx, db.DeleteFailedTaskRetentionBatchParams{
			AsOf: asOf, FailedDays: 30, BatchSize: taskRetentionMaxBatchSize,
		})
		if err != nil {
			t.Fatalf("delete failed batch: %v", err)
		}
		got := deletedSet(t, ids)
		if got[protectedParent] {
			t.Errorf("child lineage: parent with non-terminal child should be protected")
		}
		if !got[deletableParent] {
			t.Errorf("child lineage: parent whose child is terminal should be deletable")
		}
		if got[midParent] {
			t.Errorf("child lineage: mid-chain parent with a running grandchild should be protected")
		}
		if !got[grandparent] {
			t.Errorf("child lineage: grandparent whose direct child is terminal should be deletable (lineage is single-hop)")
		}
	})
}

// TestTaskRetentionBatchCapAndDrain proves the hard batch cap (no single delete
// exceeds 500), that the full-drain loop clears more rows than one batch, and
// that a rerun after the drain deletes nothing (idempotent).
func TestTaskRetentionBatchCapAndDrain(t *testing.T) {
	requireDBTest(t)
	env := newRetentionEnv(t)

	env.withRetentionTx(t, func(ctx context.Context, tx pgx.Tx, qtx *db.Queries, asOf pgtype.Timestamptz, iso string) {
		const n = 1201
		old := agedExpr(iso, retentionFarPastDays)
		mine := make(map[string]bool, n)
		// Bulk-insert n far-past failed rows in one statement.
		if _, err := tx.Exec(ctx,
			fmt.Sprintf("INSERT INTO agent_task_queue (agent_id, status, priority, completed_at) SELECT $1, 'failed', 0, %s FROM generate_series(1, %d)", old, n),
			env.agentID); err != nil {
			t.Fatalf("bulk insert: %v", err)
		}
		// Record which ids are mine (the far-past ones).
		rows, err := tx.Query(ctx,
			fmt.Sprintf("SELECT id FROM agent_task_queue WHERE agent_id = $1 AND status = 'failed' AND completed_at = %s", old),
			env.agentID)
		if err != nil {
			t.Fatalf("select mine: %v", err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				t.Fatalf("scan: %v", err)
			}
			mine[id] = true
		}
		rows.Close()
		if len(mine) != n {
			t.Fatalf("expected %d inserted rows, found %d", n, len(mine))
		}

		calls := 0
		drainedMine := 0
		lastDeleted := -1
		for {
			ids, err := qtx.DeleteFailedTaskRetentionBatch(ctx, db.DeleteFailedTaskRetentionBatchParams{
				AsOf: asOf, FailedDays: 30, BatchSize: taskRetentionMaxBatchSize,
			})
			if err != nil {
				t.Fatalf("delete batch: %v", err)
			}
			calls++
			lastDeleted = len(ids)
			if len(ids) > int(taskRetentionMaxBatchSize) {
				t.Fatalf("batch returned %d rows, exceeds cap %d", len(ids), taskRetentionMaxBatchSize)
			}
			for _, u := range ids {
				if mine[util.UUIDToString(u)] {
					drainedMine++
				}
			}
			if len(ids) == 0 {
				break
			}
			if calls > 50 {
				t.Fatalf("drain did not converge after %d calls", calls)
			}
		}
		if drainedMine != n {
			t.Errorf("drained %d of my rows, want %d", drainedMine, n)
		}
		if calls < 3 {
			t.Errorf("expected at least 3 batches to drain %d rows at cap %d, got %d", n, taskRetentionMaxBatchSize, calls)
		}
		if lastDeleted != 0 {
			t.Errorf("idempotent rerun: final batch deleted %d rows, want 0", lastDeleted)
		}
	})
}

// TestTaskRetentionAdvisoryLockSingleton proves only one holder can hold the
// retention advisory lock at a time, and that the lock is released on Close.
func TestTaskRetentionAdvisoryLockSingleton(t *testing.T) {
	requireDBTest(t)
	ctx := context.Background()

	first, ok, err := acquireTaskRetentionLock(ctx, testPool)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if !ok {
		t.Fatalf("first acquire should have obtained the lock")
	}

	_, ok2, err := acquireTaskRetentionLock(ctx, testPool)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if ok2 {
		t.Fatalf("second acquire must not obtain a lock already held")
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}

	third, ok3, err := acquireTaskRetentionLock(ctx, testPool)
	if err != nil {
		t.Fatalf("third acquire: %v", err)
	}
	if !ok3 {
		t.Fatalf("third acquire should succeed after the lock is released")
	}
	if err := third.Close(); err != nil {
		t.Fatalf("close third: %v", err)
	}
}

// TestTaskRetentionDryRunRoundZeroDeletes proves a full dry-run round deletes
// nothing: committed far-past failed and completed rows still exist after the
// round. Deletion correctness is covered by the transactional cases above.
func TestTaskRetentionDryRunRoundZeroDeletes(t *testing.T) {
	requireDBTest(t)
	fx := testutil.New(testPool, testWorkspaceID, testUserID)
	agentID := fx.Agent(t, "task-retention-dryrun-agent", "")

	failedID := fx.Task(t, agentID, testutil.Cols{
		"status":       "failed",
		"completed_at": testutil.Raw("now() - interval '3650 days'"),
	})
	completedID := fx.Task(t, agentID, testutil.Cols{
		"status":       "completed",
		"completed_at": testutil.Raw("now() - interval '3650 days'"),
	})

	cfg := taskRetentionConfig{
		FailedDays:    taskRetentionDefaultFailedDays,
		CompletedDays: taskRetentionDefaultCompletedDays,
		Interval:      taskRetentionDefaultInterval,
		BatchSize:     taskRetentionDefaultBatchSize,
		DryRun:        true,
	}
	sweepTaskRetention(context.Background(), testPool, db.New(testPool), obsmetrics.NewBusinessMetrics(), cfg)

	for name, id := range map[string]string{"failed": failedID, "completed": completedID} {
		n := fx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE id = $1", id)
		if n != 1 {
			t.Errorf("dry-run deleted the %s row (count=%d), expected it to survive", name, n)
		}
	}
}

// TestTaskRetentionDeleteRoundDrains exercises the full delete-mode round
// (sweepTaskRetention → runTaskRetentionStage drain loop → deleteTaskRetentionBatch)
// against committed fixtures: aged failed and completed rows are purged, and a
// second round is a no-op (idempotent). This covers the Go drain/recount path
// that the transactional query-level cases do not run.
func TestTaskRetentionDeleteRoundDrains(t *testing.T) {
	requireDBTest(t)
	fx := testutil.New(testPool, testWorkspaceID, testUserID)
	agentID := fx.Agent(t, "task-retention-delete-agent", "")

	failedID := fx.Task(t, agentID, testutil.Cols{
		"status":       "failed",
		"completed_at": testutil.Raw("now() - interval '3650 days'"),
	})
	completedID := fx.Task(t, agentID, testutil.Cols{
		"status":       "completed",
		"completed_at": testutil.Raw("now() - interval '3650 days'"),
	})
	// A young failed row must survive the delete round.
	freshID := fx.Task(t, agentID, testutil.Cols{
		"status":       "failed",
		"completed_at": testutil.Raw("now()"),
	})

	cfg := taskRetentionConfig{
		FailedDays:    taskRetentionDefaultFailedDays,
		CompletedDays: taskRetentionDefaultCompletedDays,
		Interval:      taskRetentionDefaultInterval,
		BatchSize:     taskRetentionDefaultBatchSize,
		DryRun:        false,
	}
	sweepTaskRetention(context.Background(), testPool, db.New(testPool), obsmetrics.NewBusinessMetrics(), cfg)

	for name, id := range map[string]string{"failed": failedID, "completed": completedID} {
		if n := fx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE id = $1", id); n != 0 {
			t.Errorf("delete round left the aged %s row (count=%d), expected it purged", name, n)
		}
	}
	if n := fx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE id = $1", freshID); n != 1 {
		t.Errorf("delete round purged the fresh row (count=%d), expected it to survive", n)
	}

	// Idempotent rerun: nothing left to delete, no error, fresh row still there.
	sweepTaskRetention(context.Background(), testPool, db.New(testPool), obsmetrics.NewBusinessMetrics(), cfg)
	if n := fx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE id = $1", freshID); n != 1 {
		t.Errorf("idempotent rerun purged the fresh row (count=%d)", n)
	}
}
