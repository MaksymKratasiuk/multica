package service

import (
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/pkg/taskfailure"
)

// SE-37711 / SE-37664: pure decision core for the runtime-scoped provider
// circuit breaker and the ordered-pool fallback selector. Everything here is
// side-effect free so the hard invariants (I5-I9) are unit-tested without a
// database; the service layer calls these from the terminal task-completion
// callback and from autopilot admission.

const (
	// circuitQuotaGrace pads a parsed quota reset so a run does not fire the
	// same instant the window is meant to reopen and lose the race.
	circuitQuotaGrace = time.Minute
	// circuitOpaqueQuotaInterval is the probe cadence for a quota refusal that
	// carries no parseable reset clause (e.g. weekly caps). Anchored to the
	// failure time, one attempt per interval per runtime keeps the window
	// observable without a per-tick failure storm.
	circuitOpaqueQuotaInterval = time.Hour
	// circuitAuthHoldWindow bounds an auth/access hold. Auth never self-heals on
	// a schedule, so the window is deliberately short: relogin is the real fix
	// (Option A — auth never auto-switches runtime), and after the window one
	// dispatch fails per cycle to keep the revocation visible in telemetry.
	circuitAuthHoldWindow = 4 * time.Hour
)

// Circuit failure-class and reset-source labels persisted on the
// runtime_provider_circuit row and surfaced in the visible fallback audit.
const (
	circuitClassQuota = "provider_quota_limit"
	circuitClassAuth  = "provider_auth_or_access"

	circuitResetParseable = "parseable"
	circuitResetOpaque    = "opaque"
	circuitResetAuth      = "auth"
)

// circuitDecision is the transition a single terminal failure implies for the
// runtime's provider circuit. Open is false for every non quota/auth class
// (I7: generic retry policy is never widened) and for a quota/auth failure
// whose hold window has already elapsed by the time it is classified.
type circuitDecision struct {
	Open         bool
	FailureClass string
	ResetSource  string
	HoldUntil    time.Time
}

// classifyCircuitFailure maps a terminal failure onto a circuit transition.
//
//   - provider_quota_limit with a parseable reset -> hold to reset + grace;
//   - provider_quota_limit without one -> hold to failedAt + opaque interval;
//   - provider_auth_or_access -> hold to failedAt + auth window (never switches);
//   - anything else -> no circuit.
//
// failedAt is the failing task's completion time; now is the classification
// instant. A hold that is already in the past opens nothing.
func classifyCircuitFailure(failureReason, errorText string, failedAt, now time.Time) circuitDecision {
	failedAt = failedAt.UTC()
	now = now.UTC()
	switch failureReason {
	case string(taskfailure.ReasonAgentProviderQuotaLimit):
		if resetAt, ok := taskfailure.ParseQuotaResetAt(errorText, now); ok && resetAt.After(now) {
			return circuitDecision{
				Open:         true,
				FailureClass: circuitClassQuota,
				ResetSource:  circuitResetParseable,
				HoldUntil:    resetAt.Add(circuitQuotaGrace),
			}
		}
		if hold := failedAt.Add(circuitOpaqueQuotaInterval); hold.After(now) {
			return circuitDecision{
				Open:         true,
				FailureClass: circuitClassQuota,
				ResetSource:  circuitResetOpaque,
				HoldUntil:    hold,
			}
		}
		return circuitDecision{}
	case string(taskfailure.ReasonAgentProviderAuthOrAccess):
		if hold := failedAt.Add(circuitAuthHoldWindow); hold.After(now) {
			return circuitDecision{
				Open:         true,
				FailureClass: circuitClassAuth,
				ResetSource:  circuitResetAuth,
				HoldUntil:    hold,
			}
		}
		return circuitDecision{}
	default:
		return circuitDecision{}
	}
}

// circuitHeld reports whether a circuit in the given state parks dispatch for
// an ordinary admitting agent. A closed circuit never holds. An open circuit
// holds until the DB transition to half_open reserves a probe; a half_open
// circuit holds every agent except the single probe holder (I5) — the probe
// lease is owned at the DB layer, so from an admitting agent's view both states
// mean the runtime is unavailable.
func circuitHeld(state string) bool {
	switch state {
	case "open", "half_open":
		return true
	default:
		return false
	}
}

// circuitHeldAt is the reset-aware hold decision the autopilot selector uses at
// dispatch time (SE-37711 / SE-37664). Unlike circuitHeld it consults the reset
// window so an ordered pool returns to a higher-priority runtime as soon as its
// quota window elapses:
//
//   - closed: never holds.
//   - half_open: holds. A probe is already testing the provider; a second
//     concurrent automatic dispatch must not pile onto it.
//   - open: holds until reset_at elapses. Once the window has passed the runtime
//     is eligible again — the next task pinned there is the natural probe, whose
//     terminal success closes the circuit and whose fresh failure reopens it with
//     a new window (I7). An open row with no reset_at holds (nothing says when it
//     is safe to retry).
//
// Autopilot dispatches are paced by their triggers, not a herd, so this
// terminal-callback-driven probe needs no separate half-open lease on this path;
// the exactly-one-probe lease (AcquireRuntimeProviderHalfOpenProbe) is reserved
// for a future claim-path integration where many queued tasks would retry at
// once.
func circuitHeldAt(state string, resetAt time.Time, resetKnown bool, now time.Time) bool {
	switch state {
	case "half_open":
		return true
	case "open":
		return !resetKnown || resetAt.After(now)
	default:
		return false
	}
}

// fallbackOutcome is the result class of selecting a runtime from an agent's
// ordered pool during automatic dispatch.
type fallbackOutcome int

const (
	// fallbackEmptyPool: the agent has no bindings (and no legacy runtime).
	fallbackEmptyPool fallbackOutcome = iota
	// fallbackSelected: an eligible runtime was chosen.
	fallbackSelected
	// fallbackAllHeld: every eligible-by-readiness binding is under a provider
	// hold. Automatic dispatch records one skipped run (ReasonDeferred) with
	// the earliest known reset (I9).
	fallbackAllHeld
	// fallbackNoneAvailable: no binding is held, but none is ready either
	// (offline / access-denied). This is a readiness skip, not a breaker skip.
	fallbackNoneAvailable
)

// runtimeCandidate is one binding in an agent's ordered pool with the
// eligibility facts the selector needs, resolved by the caller from the
// readiness verdict and the runtime's circuit row.
type runtimeCandidate struct {
	RuntimeID pgtype.UUID
	Priority  int64
	// Available is true when the readiness verdict admits the runtime (online,
	// access-valid). A candidate must be Available AND not Held to be chosen.
	Available bool
	// Held is true when the runtime's provider circuit still parks dispatch.
	Held bool
	// HoldUntil is when the hold lapses, for earliest-reset reporting. The zero
	// value means "unknown" (e.g. a half_open probe with no deadline) and must
	// be surfaced as "manual action required", never fabricated into a date
	// (I9).
	HoldUntil time.Time
}

// fallbackDecision is the selector's verdict over an ordered candidate list.
type fallbackDecision struct {
	Outcome fallbackOutcome
	// Chosen is valid only when Outcome == fallbackSelected.
	Chosen runtimeCandidate
	// EarliestReset / EarliestKnown describe the soonest hold lapse across the
	// held candidates when Outcome == fallbackAllHeld. EarliestKnown is false
	// when no held candidate carries a known deadline.
	EarliestReset time.Time
	EarliestKnown bool
	HeldCount     int
}

// selectFallbackRuntime picks the first ready, unheld runtime from an agent's
// ordered pool. Candidates must already be sorted by (priority, id). It never
// mutates its input and makes no I/O, so it is exhaustively unit-tested for
// I5 (hold scoped per runtime), I8 (caller bypasses this for manual runs), and
// I9 (all-held reporting without fabricated timestamps).
func selectFallbackRuntime(candidates []runtimeCandidate) fallbackDecision {
	if len(candidates) == 0 {
		return fallbackDecision{Outcome: fallbackEmptyPool}
	}
	var (
		heldCount     int
		earliest      time.Time
		earliestKnown bool
	)
	for _, c := range candidates {
		if c.Available && !c.Held {
			return fallbackDecision{Outcome: fallbackSelected, Chosen: c}
		}
		if c.Held {
			heldCount++
			if !c.HoldUntil.IsZero() && (!earliestKnown || c.HoldUntil.Before(earliest)) {
				earliest = c.HoldUntil
				earliestKnown = true
			}
		}
	}
	if heldCount > 0 {
		return fallbackDecision{
			Outcome:       fallbackAllHeld,
			EarliestReset: earliest,
			EarliestKnown: earliestKnown,
			HeldCount:     heldCount,
		}
	}
	return fallbackDecision{Outcome: fallbackNoneAvailable}
}
