package service

import (
	"strings"
	"testing"
)

// TestRunOnlyTriggerSummary pins the visible-failover contract (SE-37711 /
// SE-37664, parent §5: never silent). A run_only dispatch routed to a non-default
// runtime must carry the failover marker in its task snapshot, the marker must
// survive title truncation, and a run that did not fall over must look exactly
// like before.
func TestRunOnlyTriggerSummary(t *testing.T) {
	markerRunes := len([]rune(fallbackSummaryMarker))

	t.Run("no fallback leaves the snapshot untouched", func(t *testing.T) {
		if got := runOnlyTriggerSummary("Nightly triage", false); got != "Nightly triage" {
			t.Fatalf("got %q, want the plain title", got)
		}
	})

	t.Run("no fallback still truncates an over-long title", func(t *testing.T) {
		long := strings.Repeat("x", triggerSummaryMaxLen+50)
		got := runOnlyTriggerSummary(long, false)
		if r := []rune(got); len(r) != triggerSummaryMaxLen+1 || !strings.HasSuffix(got, "…") {
			t.Fatalf("got %d runes (suffix …=%v), want %d + ellipsis", len(r), strings.HasSuffix(got, "…"), triggerSummaryMaxLen)
		}
	})

	t.Run("fallback appends a visible marker", func(t *testing.T) {
		got := runOnlyTriggerSummary("Nightly triage", true)
		if got != "Nightly triage"+fallbackSummaryMarker {
			t.Fatalf("got %q, want title + marker", got)
		}
	})

	t.Run("fallback marker survives on an empty title", func(t *testing.T) {
		if got := runOnlyTriggerSummary("", true); got != fallbackSummaryMarker {
			t.Fatalf("got %q, want the bare marker", got)
		}
	})

	t.Run("fallback keeps the whole marker within the length budget", func(t *testing.T) {
		long := strings.Repeat("у", triggerSummaryMaxLen+50) // multibyte to prove rune budgeting
		got := runOnlyTriggerSummary(long, true)
		if r := []rune(got); len(r) > triggerSummaryMaxLen {
			t.Fatalf("summary is %d runes, want <= %d", len(r), triggerSummaryMaxLen)
		}
		if !strings.HasSuffix(got, fallbackSummaryMarker) {
			t.Fatalf("marker was truncated away: %q", got)
		}
		// The title portion is truncated with an ellipsis and the full marker
		// is preserved: budget = max - markerRunes, title fills the budget.
		wantTitleRunes := triggerSummaryMaxLen - markerRunes
		title := strings.TrimSuffix(got, fallbackSummaryMarker)
		if r := []rune(title); len(r) != wantTitleRunes || !strings.HasSuffix(title, "…") {
			t.Fatalf("title portion = %d runes (ellipsis=%v), want %d + ellipsis", len(r), strings.HasSuffix(title, "…"), wantTitleRunes)
		}
	})
}
