package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// SE-37711 / SE-37664: wire the pure circuit decision core (runtime_circuit.go)
// into the terminal task callbacks. A terminal provider quota/auth failure opens
// the failing runtime's provider circuit; a terminal success closes it. Both are
// best-effort, post-commit side effects: the terminal status has already been
// persisted, so a circuit write must never turn a committed completion into an
// error. Every failure here is logged and swallowed. Idempotency is owned by the
// queries — a duplicate or late callback for an older task is a no-op (zero rows
// returned), and a stale success cannot close a fresher open circuit.

// syncRuntimeCircuitOnFailure opens the runtime's provider circuit when a
// terminal failure is a provider quota or auth/access refusal (invariant I7).
// It only reaches the DB for those two failure classes; every other reason
// resolves to "no circuit" in the pure classifier and returns before any I/O.
func (s *TaskService) syncRuntimeCircuitOnFailure(ctx context.Context, task db.AgentTaskQueue, failureReason, errMsg string) {
	if !task.RuntimeID.Valid {
		return
	}
	now := time.Now().UTC()
	failedAt := now
	if task.CompletedAt.Valid {
		failedAt = task.CompletedAt.Time.UTC()
	}
	decision := classifyCircuitFailure(failureReason, errMsg, failedAt, now)
	if !decision.Open {
		return
	}
	runtime, err := s.Queries.GetAgentRuntime(ctx, task.RuntimeID)
	if err != nil {
		slog.Warn("runtime circuit: load runtime for open failed",
			"task_id", util.UUIDToString(task.ID),
			"runtime_id", util.UUIDToString(task.RuntimeID),
			"error", err)
		return
	}
	rows, err := s.Queries.OpenRuntimeProviderCircuit(ctx, db.OpenRuntimeProviderCircuitParams{
		WorkspaceID:        runtime.WorkspaceID,
		RuntimeID:          task.RuntimeID,
		Provider:           runtime.Provider,
		Reason:             pgtype.Text{String: decision.FailureClass, Valid: true},
		ResetAt:            pgtype.Timestamptz{Time: decision.HoldUntil, Valid: true},
		FailureCompletedAt: pgtype.Timestamptz{Time: failedAt, Valid: true},
		FailureTaskID:      task.ID,
	})
	if err != nil {
		slog.Warn("runtime circuit: open failed",
			"task_id", util.UUIDToString(task.ID),
			"runtime_id", util.UUIDToString(task.RuntimeID),
			"provider", runtime.Provider,
			"error", err)
		return
	}
	if len(rows) == 0 {
		// An equal-or-newer failure already owns the current generation: this is
		// a duplicate/late callback for an older task. Nothing changed.
		return
	}
	slog.Info("runtime provider circuit opened",
		"task_id", util.UUIDToString(task.ID),
		"runtime_id", util.UUIDToString(task.RuntimeID),
		"provider", runtime.Provider,
		"failure_class", decision.FailureClass,
		"reset_source", decision.ResetSource,
		"reset_at", decision.HoldUntil,
		"generation", rows[0].Generation)
}

// syncRuntimeCircuitOnSuccess closes the runtime's provider circuit after a
// terminal success. This is what promotes an open/half_open circuit back to
// closed once a probe (or any run) on the runtime succeeds. It is a no-op when
// the circuit is already closed or the success is stale relative to the failure
// that opened the current generation.
func (s *TaskService) syncRuntimeCircuitOnSuccess(ctx context.Context, task db.AgentTaskQueue) {
	if !task.RuntimeID.Valid {
		return
	}
	runtime, err := s.Queries.GetAgentRuntime(ctx, task.RuntimeID)
	if err != nil {
		slog.Warn("runtime circuit: load runtime for close failed",
			"task_id", util.UUIDToString(task.ID),
			"runtime_id", util.UUIDToString(task.RuntimeID),
			"error", err)
		return
	}
	completedAt := time.Now().UTC()
	if task.CompletedAt.Valid {
		completedAt = task.CompletedAt.Time.UTC()
	}
	rows, err := s.Queries.CloseRuntimeProviderCircuitOnSuccess(ctx, db.CloseRuntimeProviderCircuitOnSuccessParams{
		RuntimeID:          task.RuntimeID,
		Provider:           runtime.Provider,
		SuccessCompletedAt: pgtype.Timestamptz{Time: completedAt, Valid: true},
		SuccessTaskID:      task.ID,
	})
	if err != nil {
		slog.Warn("runtime circuit: close on success failed",
			"task_id", util.UUIDToString(task.ID),
			"runtime_id", util.UUIDToString(task.RuntimeID),
			"provider", runtime.Provider,
			"error", err)
		return
	}
	if len(rows) == 0 {
		// Already closed, or this success predates the current failure epoch.
		return
	}
	slog.Info("runtime provider circuit closed on success",
		"task_id", util.UUIDToString(task.ID),
		"runtime_id", util.UUIDToString(task.RuntimeID),
		"provider", runtime.Provider,
		"generation", rows[0].Generation)
}
