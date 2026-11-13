package fusee

import (
	"context"
	"encoding/json"
	"time"
)

const (
	scheduleType    = "fusee.schedule"
	scheduleControl = "fusee.schedule.control"
)

// ScheduleOptions describe when a schedule starts runs: a cron expression
// (in Timezone) or an interval.
type ScheduleOptions struct {
	Cron     string
	Every    time.Duration
	Offset   time.Duration
	Timezone string // default "UTC"
	// "skip" (the default), "buffer-one", "buffer-all", "cancel-other",
	// "terminate-other" or "allow-all".
	Overlap string
	Jitter  time.Duration
	// How far back missed times are caught up (default a year).
	CatchupWindow time.Duration
	Paused        bool
}

// Schedule is a schedule, addressed by name.
type Schedule struct {
	e    *Engine
	Name string
	ID   string
}

// Schedule creates a schedule (or updates its spec) that starts workflow
// with input on its cron expression or interval.
func (e *Engine) Schedule(ctx context.Context, name, workflow string, input any, o ScheduleOptions) (*Schedule, error) {
	var cron, every any
	if o.Cron != "" {
		cron = o.Cron
	}
	if o.Every > 0 {
		every = ms(o.Every)
	}
	tz := o.Timezone
	if tz == "" {
		tz = "UTC"
	}
	overlap := o.Overlap
	if overlap == "" {
		overlap = "skip"
	}
	catchup := ms(o.CatchupWindow)
	if catchup == 0 {
		catchup = 365 * 86_400_000
	}
	spec := map[string]any{
		"cron": cron, "everyMs": every, "offsetMs": ms(o.Offset), "timezone": tz,
		"overlap": overlap, "jitterMs": ms(o.Jitter), "catchupWindowMs": catchup,
		"paused": o.Paused, "workflow": workflow, "input": input,
	}
	b, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	if err := validateSchedule(b); err != nil {
		return nil, err
	}
	s := e.ScheduleHandle(name)
	start, err := json.Marshal(map[string]any{"name": name, "spec": json.RawMessage(b)})
	if err != nil {
		return nil, err
	}
	if err := e.startWire(ctx, scheduleType, 1, s.ID, start, StartOptions{IfRunning: IfRunningUseExisting, Version: 1}); err != nil {
		return nil, err
	}
	if err := s.send(ctx, map[string]any{"op": "update", "spec": json.RawMessage(b)}); err != nil {
		return nil, err
	}
	return s, nil
}

// ScheduleHandle is a handle to an existing schedule.
func (e *Engine) ScheduleHandle(name string) *Schedule {
	return &Schedule{e: e, Name: name, ID: "fusee.schedule/" + name}
}

func (s *Schedule) send(ctx context.Context, cmd map[string]any) error {
	return s.e.Run(s.ID).Signal(ctx, scheduleControl, cmd)
}

// Pause stops starting runs until Resume.
func (s *Schedule) Pause(ctx context.Context) error {
	return s.send(ctx, map[string]any{"op": "pause"})
}

// Resume starts runs again.
func (s *Schedule) Resume(ctx context.Context) error {
	return s.send(ctx, map[string]any{"op": "resume"})
}

// Trigger starts one run now.
func (s *Schedule) Trigger(ctx context.Context) error {
	return s.send(ctx, map[string]any{"op": "trigger"})
}

// Backfill starts a run for every scheduled time in [from, to].
func (s *Schedule) Backfill(ctx context.Context, from, to time.Time) error {
	return s.send(ctx, map[string]any{"op": "backfill", "fromMs": from.UnixMilli(), "toMs": to.UnixMilli()})
}

// Describe is the schedule's spec and state.
func (s *Schedule) Describe(ctx context.Context) (json.RawMessage, error) {
	p, err := s.e.request(ctx, true, func(tok uint64) error {
		return s.e.n.queryNative(s.ID, "describe", []byte("null"), tok)
	})
	return raw(p), err
}

// Delete stops the schedule; runs it started keep running.
func (s *Schedule) Delete(ctx context.Context) error {
	return s.e.Run(s.ID).Terminate(ctx, "schedule deleted")
}
