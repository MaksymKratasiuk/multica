package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// SE-37711 / SE-37664: runtime selection for automatic run_only dispatch.
//
// This is the breaker-aware replacement for a bare AgentReadiness call on the
// automatic path. It walks the agent's ordered runtime pool and returns the
// first runtime that is both ready (online + access-valid) and not parked by a
// provider circuit hold, so a quota/auth refusal on the primary runtime fails
// over to the next binding instead of stalling the agent.
//
// The pure ordered pick and its all-held reporting live in runtime_circuit.go
// (selectFallbackRuntime); this method only resolves the eligibility facts each
// candidate needs from the runtime row and its circuit.

// selectPoolRuntime resolves which runtime an automatic run_only dispatch should
// pin from the agent's ordered pool.
//
// The pool is the agent's agent_runtime_binding rows in (priority, id) order; an
// agent with no binding rows falls back to the singleton pool implied by
// agent.runtime_id, so legacy single-runtime agents behave exactly as before
// (backfill migration 506 also keeps agent.runtime_id present as the priority-0
// binding once bindings exist, so the two views agree).
//
// firstVerdict is the readiness verdict of the highest-priority candidate; the
// caller uses it to phrase a fallbackNoneAvailable skip the same way the legacy
// single-runtime gate did.
//
// A NEW binding, runtime, or circuit lookup error fails CLOSED (returns err):
// a selector that cannot read the breaker must not route work past it. The
// callers map that error the same way they already map an AgentReadiness DB
// error — the admission gate fails open (a transient hiccup must not swallow a
// scheduled run, and nothing is routed yet), and dispatchRunOnly fails the run
// (no task is created, so no work lands on a runtime whose circuit we could not
// read).
func (s *AutopilotService) selectPoolRuntime(ctx context.Context, agent db.Agent) (fallbackDecision, AgentVerdict, error) {
	now := time.Now().UTC()
	lookup := s.runtimeLookup()

	bindings, err := s.Queries.ListAgentRuntimeBindings(ctx, agent.ID)
	if err != nil {
		return fallbackDecision{}, AgentVerdict{}, fmt.Errorf("list agent runtime bindings: %w", err)
	}

	var runtimeIDs []pgtype.UUID
	if len(bindings) == 0 {
		if !agent.RuntimeID.Valid {
			return fallbackDecision{Outcome: fallbackEmptyPool}, AgentVerdict{}, nil
		}
		runtimeIDs = []pgtype.UUID{agent.RuntimeID}
	} else {
		runtimeIDs = make([]pgtype.UUID, len(bindings))
		for i, b := range bindings {
			runtimeIDs[i] = b.RuntimeID
		}
	}

	candidates := make([]runtimeCandidate, 0, len(runtimeIDs))
	var firstVerdict AgentVerdict
	for i, rid := range runtimeIDs {
		rt, err := lookup.Get(ctx, rid)
		if err != nil {
			return fallbackDecision{}, AgentVerdict{}, fmt.Errorf("load runtime %s: %w", util.UUIDToString(rid), err)
		}
		verdict := runtimeVerdict(rt, agent)
		if i == 0 {
			firstVerdict = verdict
		}

		held := false
		var holdUntil time.Time
		circuit, err := s.Queries.GetRuntimeProviderCircuit(ctx, db.GetRuntimeProviderCircuitParams{
			RuntimeID: rid,
			Provider:  rt.Provider,
		})
		switch {
		case err == nil:
			held = circuitHeldAt(circuit.State, circuit.ResetAt.Time, circuit.ResetAt.Valid, now)
			// Only an open hold carries a meaningful deadline; a half_open probe
			// has no fixed reset, so its HoldUntil stays zero ("unknown", I9).
			if held && circuit.State == "open" && circuit.ResetAt.Valid {
				holdUntil = circuit.ResetAt.Time
			}
		case errors.Is(err, pgx.ErrNoRows):
			// No circuit row means the breaker has never opened for this runtime:
			// closed, not held.
		default:
			return fallbackDecision{}, AgentVerdict{}, fmt.Errorf("load runtime circuit %s: %w", util.UUIDToString(rid), err)
		}

		candidates = append(candidates, runtimeCandidate{
			RuntimeID: rid,
			Priority:  int64(i),
			Available: verdict.Ready(),
			Held:      held,
			HoldUntil: holdUntil,
		})
	}

	return selectFallbackRuntime(candidates), firstVerdict, nil
}

// poolAllHeldReason phrases the deferral message when every ready runtime in the
// agent's pool is under a provider circuit hold. It surfaces the earliest known
// reset so the skipped-run record and the failure monitor show when work can
// resume; when no held runtime carries a known deadline (e.g. a half_open probe
// in flight) it says manual action is required rather than fabricate a time (I9).
func poolAllHeldReason(ap db.Autopilot, decision fallbackDecision) string {
	who := "assignee agent"
	if ap.AssigneeType == "squad" {
		who = "squad leader"
	}
	if decision.EarliestKnown {
		return fmt.Sprintf(
			"%s: all %d bound runtime(s) held by provider circuit; earliest reset at %s",
			who, decision.HeldCount, decision.EarliestReset.UTC().Format(time.RFC3339),
		)
	}
	return fmt.Sprintf(
		"%s: all %d bound runtime(s) held by provider circuit; reset time unknown, manual action required",
		who, decision.HeldCount,
	)
}
