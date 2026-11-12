// A scheduled job in Go: a schedule starts the report workflow on a cron
// expression; each scheduled time runs once, even across restarts.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	fusee "github.com/bitsinfra/fusee/bindings/go"
)

var sent atomic.Int32

var Report = fusee.Define("report", func(c *fusee.Ctx, _ any) (struct{}, error) {
	id := c.WorkflowID()
	_, err := fusee.Step(c, "send-report", func(context.Context) (bool, error) {
		fmt.Println(" ", id, "sent")
		return true, nil
	})
	sent.Add(1)
	return struct{}{}, err
})

func main() {
	e, err := fusee.Open(fusee.Options{Dir: "./data/scheduled-job", Workflows: []fusee.Definition{Report}})
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	// A real job: fusee.ScheduleOptions{Cron: "0 2 * * *", Timezone: "Europe/London"}.
	s, err := e.Schedule(ctx, "nightly-report", "report", nil, fusee.ScheduleOptions{Every: time.Second, Overlap: "skip"})
	if err != nil {
		log.Fatal(err)
	}
	for sent.Load() < 3 {
		time.Sleep(50 * time.Millisecond)
	}
	raw, err := s.Describe(ctx)
	if err != nil {
		log.Fatal(err)
	}
	var d struct {
		NextMs int64 `json:"nextMs"`
	}
	_ = json.Unmarshal(raw, &d)
	fmt.Println("next:", d.NextMs)
	if err := s.Delete(ctx); err != nil {
		log.Fatal(err)
	}
}
