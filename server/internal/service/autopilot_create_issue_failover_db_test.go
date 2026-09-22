package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// TestDispatchCreateIssueBreakerAwareRuntimeSelection pins F5 (SE-37711 /
// SE-37664): the scheduled/webhook create_issue path must consult the breaker
// like dispatchRunOnly does, and it must decide BEFORE the issue is created.
//
//   - a quota hold on the agent's home runtime falls over to the healthy
//     fallback: the created issue's task is pinned to the fallback runtime, and
//     exactly one visible system comment narrates the failover (never silent);
//   - an auth hold on the home runtime is not a failover cause, so the dispatch
//     is skipped up front and leaves no doomed issue+task nobody can run.
//
// It is DB-backed: it drives the real dispatchCreateIssue transaction, so it
// skips locally without a database and is validated by backend-ci.
func TestDispatchCreateIssueBreakerAwareRuntimeSelection(t *testing.T) {
	pool := sharedTestPool(t)
	ctx := context.Background()
	q := db.New(pool)
	svc := &AutopilotService{
		Queries:   q,
		TxStarter: pool,
		Bus:       events.New(),
		TaskSvc:   &TaskService{Queries: q, TxStarter: pool, Bus: events.New()},
	}

	suffix := time.Now().UnixNano()
	userID := insertBindingTeardownRow(t, pool, `
		INSERT INTO "user" (name, email) VALUES ($1, $2) RETURNING id`,
		"CreateIssue FO", fmt.Sprintf("ci-fo-%d@multica.ai", suffix))
	workspaceID := insertBindingTeardownRow(t, pool, `
		INSERT INTO workspace (name, slug, description, issue_prefix)
		VALUES ($1, $2, '', 'CIF') RETURNING id`,
		"CreateIssue FO", fmt.Sprintf("ci-fo-%d", suffix))
	execBindingTeardown(t, pool, `INSERT INTO member (workspace_id, user_id, role) VALUES ($1, $2, 'owner')`, workspaceID, userID)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM member WHERE workspace_id = $1`, workspaceID)
		_, _ = pool.Exec(ctx, `DELETE FROM workspace WHERE id = $1`, workspaceID)
		_, _ = pool.Exec(ctx, `DELETE FROM "user" WHERE id = $1`, userID)
	})

	// insertCircuitReason opens the home runtime's circuit with a failure class so
	// the selector applies the F3 auth-no-switch / quota-falls-through rule off it.
	insertCircuitReason := func(runtimeID, reason string, resetAt time.Time) {
		t.Helper()
		execBindingTeardown(t, pool, `
			INSERT INTO runtime_provider_circuit (workspace_id, runtime_id, provider, state, reason, reset_at, opened_at)
			VALUES ($1, $2, 'binding_teardown_test', 'open', $3, $4, now())`,
			workspaceID, runtimeID, reason, resetAt)
		t.Cleanup(func() { _, _ = pool.Exec(ctx, `DELETE FROM runtime_provider_circuit WHERE runtime_id = $1`, runtimeID) })
	}

	// seedAP builds a create_issue autopilot assigned to the agent plus a running
	// run, and returns the loaded rows dispatchCreateIssue expects.
	seedAP := func(agentID string) (db.Autopilot, db.AutopilotRun) {
		t.Helper()
		apID := insertBindingTeardownRow(t, pool, `
			INSERT INTO autopilot (workspace_id, title, assignee_type, assignee_id, status, execution_mode, created_by_type, created_by_id)
			VALUES ($1, 'failover ci ap', 'agent', $2, 'active', 'create_issue', 'member', $3) RETURNING id`,
			workspaceID, agentID, userID)
		runID := insertBindingTeardownRow(t, pool, `
			INSERT INTO autopilot_run (autopilot_id, source, status) VALUES ($1, 'schedule', 'running') RETURNING id`, apID)
		t.Cleanup(func() {
			_, _ = pool.Exec(ctx, `DELETE FROM autopilot_run WHERE id = $1`, runID)
			_, _ = pool.Exec(ctx, `DELETE FROM autopilot WHERE id = $1`, apID)
		})
		ap, err := q.GetAutopilot(ctx, util.MustParseUUID(apID))
		if err != nil {
			t.Fatalf("get autopilot: %v", err)
		}
		run, err := q.GetAutopilotRun(ctx, util.MustParseUUID(runID))
		if err != nil {
			t.Fatalf("get run: %v", err)
		}
		return ap, run
	}

	future := time.Now().UTC().Add(2 * time.Hour)

	t.Run("quota-held home runtime pins the task to the fallback and posts one system comment", func(t *testing.T) {
		r0 := insertRuntimeForBindingTeardown(t, pool, workspaceID, userID, "Q r0")
		r1 := insertRuntimeForBindingTeardown(t, pool, workspaceID, userID, "Q r1")
		agentID := insertAgentForBindingTeardown(t, pool, workspaceID, userID, "q agent", r0)
		insertBinding(t, pool, workspaceID, agentID, r0, 0)
		insertBinding(t, pool, workspaceID, agentID, r1, 1)
		insertCircuitReason(r0, circuitClassQuota, future) // home held by quota; r1 healthy

		ap, run := seedAP(agentID)
		if err := svc.dispatchCreateIssue(ctx, ap, &run, "UTC", pgtype.UUID{}); err != nil {
			t.Fatalf("dispatchCreateIssue: %v", err)
		}

		var issueID string
		if err := pool.QueryRow(ctx, `SELECT issue_id FROM autopilot_run WHERE id = $1`, run.ID).Scan(&issueID); err != nil {
			t.Fatalf("read run issue: %v", err)
		}
		t.Cleanup(func() {
			_, _ = pool.Exec(ctx, `DELETE FROM comment WHERE issue_id = $1`, issueID)
			_, _ = pool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
			_, _ = pool.Exec(ctx, `DELETE FROM issue WHERE id = $1`, issueID)
		})

		var (
			taskRuntime    string
			dispatchAudit  []byte
			triggerSummary pgtype.Text
		)
		if err := pool.QueryRow(ctx, `
			SELECT runtime_id, dispatch_runtime_audit, trigger_summary
			FROM agent_task_queue WHERE issue_id = $1`, issueID).Scan(&taskRuntime, &dispatchAudit, &triggerSummary); err != nil {
			t.Fatalf("read task runtime: %v", err)
		}
		if taskRuntime != r1 {
			t.Fatalf("task runtime = %s, want fallback r1 = %s", taskRuntime, r1)
		}
		var audit dispatchRuntimeAudit
		if err := json.Unmarshal(dispatchAudit, &audit); err != nil {
			t.Fatalf("dispatch_runtime_audit is not durable structured evidence: %q: %v", dispatchAudit, err)
		}
		if audit.SourceRuntimeID != r0 || audit.TargetRuntimeID != r1 || audit.Reason != "runtime_failover" {
			t.Fatalf("dispatch audit = %+v, want source=%s target=%s runtime_failover", audit, r0, r1)
		}
		if !triggerSummary.Valid || !strings.Contains(triggerSummary.String, "failover") {
			t.Fatalf("trigger_summary = %#v, want visible failover route", triggerSummary)
		}

		var systemComments int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM comment WHERE issue_id = $1 AND author_type = 'system'`, issueID).Scan(&systemComments); err != nil {
			t.Fatalf("count system comments: %v", err)
		}
		if systemComments != 1 {
			t.Fatalf("system comments = %d, want exactly 1 failover narration", systemComments)
		}
	})

	t.Run("auth-held home runtime skips the dispatch and creates no doomed issue", func(t *testing.T) {
		r0 := insertRuntimeForBindingTeardown(t, pool, workspaceID, userID, "A r0")
		r1 := insertRuntimeForBindingTeardown(t, pool, workspaceID, userID, "A r1")
		agentID := insertAgentForBindingTeardown(t, pool, workspaceID, userID, "a agent", r0)
		insertBinding(t, pool, workspaceID, agentID, r0, 0)
		insertBinding(t, pool, workspaceID, agentID, r1, 1)
		insertCircuitReason(r0, circuitClassAuth, future) // auth hold: no fallback, skip up front

		ap, run := seedAP(agentID)
		err := svc.dispatchCreateIssue(ctx, ap, &run, "UTC", pgtype.UUID{})
		var skipped *errDispatchSkipped
		if !errors.As(err, &skipped) {
			t.Fatalf("dispatchCreateIssue err = %v, want errDispatchSkipped (auth hold, no fallback)", err)
		}

		var issueLinked pgtype.UUID
		if err := pool.QueryRow(ctx, `SELECT issue_id FROM autopilot_run WHERE id = $1`, run.ID).Scan(&issueLinked); err != nil {
			t.Fatalf("read run issue: %v", err)
		}
		if issueLinked.Valid {
			t.Fatalf("auth-held dispatch created issue %s, want none", util.UUIDToString(issueLinked))
		}
	})

	t.Run("quota primary then auth secondary stops both automatic modes before healthy tertiary", func(t *testing.T) {
		r0 := insertRuntimeForBindingTeardown(t, pool, workspaceID, userID, "chain r0")
		r1 := insertRuntimeForBindingTeardown(t, pool, workspaceID, userID, "chain r1")
		r2 := insertRuntimeForBindingTeardown(t, pool, workspaceID, userID, "chain r2")
		agentID := insertAgentForBindingTeardown(t, pool, workspaceID, userID, "chain agent", r0)
		insertBinding(t, pool, workspaceID, agentID, r0, 0)
		insertBinding(t, pool, workspaceID, agentID, r1, 1)
		insertBinding(t, pool, workspaceID, agentID, r2, 2)
		insertCircuitReason(r0, circuitClassQuota, future)
		insertCircuitReason(r1, circuitClassAuth, future.Add(2*time.Hour))

		createAP, createRun := seedAP(agentID)
		var createSkipped *errDispatchSkipped
		if err := svc.dispatchCreateIssue(ctx, createAP, &createRun, "UTC", pgtype.UUID{}); !errors.As(err, &createSkipped) {
			t.Fatalf("create_issue err = %v, want auth-held deferred skip", err)
		}
		var issueLinked pgtype.UUID
		if err := pool.QueryRow(ctx, `SELECT issue_id FROM autopilot_run WHERE id = $1`, createRun.ID).Scan(&issueLinked); err != nil {
			t.Fatalf("read create_issue run: %v", err)
		}
		if issueLinked.Valid {
			t.Fatalf("create_issue routed past auth hold and created issue %s", util.UUIDToString(issueLinked))
		}

		runOnlyAP, runOnlyRun := seedAP(agentID)
		var runOnlySkipped *errDispatchSkipped
		if err := svc.dispatchRunOnly(ctx, runOnlyAP, &runOnlyRun, pgtype.UUID{}); !errors.As(err, &runOnlySkipped) {
			t.Fatalf("run_only err = %v, want auth-held deferred skip", err)
		}
		var tasks int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM agent_task_queue WHERE autopilot_run_id = $1`, runOnlyRun.ID).Scan(&tasks); err != nil {
			t.Fatalf("count run_only tasks: %v", err)
		}
		if tasks != 0 {
			t.Fatalf("run_only routed past auth hold and created %d task(s)", tasks)
		}
	})

	t.Run("comment write failure cannot erase durable create_issue route evidence", func(t *testing.T) {
		r0 := insertRuntimeForBindingTeardown(t, pool, workspaceID, userID, "audit r0")
		r1 := insertRuntimeForBindingTeardown(t, pool, workspaceID, userID, "audit r1")
		agentID := insertAgentForBindingTeardown(t, pool, workspaceID, userID, "audit agent", r0)
		insertBinding(t, pool, workspaceID, agentID, r0, 0)
		insertBinding(t, pool, workspaceID, agentID, r1, 1)
		insertCircuitReason(r0, circuitClassQuota, future)

		originalHook := svc.postCreateIssueFailoverCommentFn
		svc.postCreateIssueFailoverCommentFn = func(context.Context, db.Autopilot, db.Issue, db.Agent, dispatchRuntimeAudit) error {
			return errors.New("injected comment write failure")
		}
		defer func() { svc.postCreateIssueFailoverCommentFn = originalHook }()

		ap, run := seedAP(agentID)
		if err := svc.dispatchCreateIssue(ctx, ap, &run, "UTC", pgtype.UUID{}); err != nil {
			t.Fatalf("dispatchCreateIssue: %v", err)
		}
		var issueID string
		if err := pool.QueryRow(ctx, `SELECT issue_id FROM autopilot_run WHERE id = $1`, run.ID).Scan(&issueID); err != nil {
			t.Fatalf("read run issue: %v", err)
		}
		t.Cleanup(func() {
			_, _ = pool.Exec(ctx, `DELETE FROM comment WHERE issue_id = $1`, issueID)
			_, _ = pool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
			_, _ = pool.Exec(ctx, `DELETE FROM issue WHERE id = $1`, issueID)
		})

		var audit []byte
		if err := pool.QueryRow(ctx, `SELECT dispatch_runtime_audit FROM agent_task_queue WHERE issue_id = $1`, issueID).Scan(&audit); err != nil {
			t.Fatalf("read durable task audit: %v", err)
		}
		if len(audit) == 0 {
			t.Fatal("comment failure left fallback routing silent: task audit is empty")
		}
		var comments int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM comment WHERE issue_id = $1 AND author_type = 'system'`, issueID).Scan(&comments); err != nil {
			t.Fatalf("count comments: %v", err)
		}
		if comments != 0 {
			t.Fatalf("system comments = %d, want injected write failure with durable task evidence", comments)
		}
	})

	t.Run("two create_issue schedulers share exactly one half-open probe lease", func(t *testing.T) {
		runtimeID := insertRuntimeForBindingTeardown(t, pool, workspaceID, userID, "shared probe runtime")
		agentA := insertAgentForBindingTeardown(t, pool, workspaceID, userID, "probe agent A", runtimeID)
		agentB := insertAgentForBindingTeardown(t, pool, workspaceID, userID, "probe agent B", runtimeID)
		insertBinding(t, pool, workspaceID, agentA, runtimeID, 0)
		insertBinding(t, pool, workspaceID, agentB, runtimeID, 0)
		insertCircuitReason(runtimeID, circuitClassQuota, time.Now().UTC().Add(-time.Hour))

		apA, runA := seedAP(agentA)
		apB, runB := seedAP(agentB)
		runs := []*db.AutopilotRun{&runA, &runB}
		aps := []db.Autopilot{apA, apB}

		start := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		for i := range aps {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				errs <- svc.dispatchCreateIssue(ctx, aps[i], runs[i], "UTC", pgtype.UUID{})
			}(i)
		}
		close(start)
		wg.Wait()
		close(errs)

		var succeeded, deferred int
		for err := range errs {
			if err == nil {
				succeeded++
				continue
			}
			var skipped *errDispatchSkipped
			if errors.As(err, &skipped) {
				deferred++
				continue
			}
			t.Fatalf("unexpected concurrent dispatch error: %v", err)
		}
		if succeeded != 1 || deferred != 1 {
			t.Fatalf("concurrent create_issue probe results: success=%d deferred=%d, want 1/1", succeeded, deferred)
		}

		var issueIDs []string
		for _, run := range runs {
			var issueID pgtype.UUID
			if err := pool.QueryRow(ctx, `SELECT issue_id FROM autopilot_run WHERE id = $1`, run.ID).Scan(&issueID); err != nil {
				t.Fatalf("read run issue: %v", err)
			}
			if issueID.Valid {
				issueIDs = append(issueIDs, util.UUIDToString(issueID))
			}
		}
		if len(issueIDs) != 1 {
			t.Fatalf("created issues = %v, want exactly one winner issue", issueIDs)
		}
		issueID := issueIDs[0]
		t.Cleanup(func() {
			_, _ = pool.Exec(ctx, `DELETE FROM comment WHERE issue_id = $1`, issueID)
			_, _ = pool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
			_, _ = pool.Exec(ctx, `DELETE FROM issue WHERE id = $1`, issueID)
		})

		var taskID string
		if err := pool.QueryRow(ctx, `SELECT id FROM agent_task_queue WHERE issue_id = $1`, issueID).Scan(&taskID); err != nil {
			t.Fatalf("read winner task: %v", err)
		}
		var probeTaskID pgtype.UUID
		if err := pool.QueryRow(ctx, `SELECT probe_task_id FROM runtime_provider_circuit WHERE runtime_id = $1`, runtimeID).Scan(&probeTaskID); err != nil {
			t.Fatalf("read probe lease: %v", err)
		}
		if util.UUIDToString(probeTaskID) != taskID {
			t.Fatalf("probe_task_id=%s, want winner task=%s", util.UUIDToString(probeTaskID), taskID)
		}
	})
}
