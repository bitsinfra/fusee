// An AI agent loop with tool calls in Go: every model and tool call is a step.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	fusee "github.com/bitsinfra/fusee/bindings/go"
)

type Turn struct {
	Answer string `json:"answer,omitempty"`
	Tool   string `json:"tool,omitempty"`
	Args   string `json:"args,omitempty"`
}

func model(history []string) Turn {
	for _, h := range history {
		if strings.HasPrefix(h, "tool:") {
			return Turn{Answer: fmt.Sprintf("Pack an umbrella (%s).", history[len(history)-1])}
		}
	}
	return Turn{Tool: "weather", Args: "Lisbon"}
}

var tools = map[string]func(string) string{
	"weather": func(city string) string { return "tool: rain in " + city },
}

var Agent = fusee.Define("agent", func(c *fusee.Ctx, question string) (string, error) {
	history := []string{"user: " + question}
	for turn := 0; turn < 10; turn++ {
		seen := append([]string(nil), history...)
		t, err := fusee.Step(c, fmt.Sprintf("model-%d", turn), func(context.Context) (Turn, error) { return model(seen), nil })
		if err != nil {
			return "", err
		}
		if t.Answer != "" {
			return t.Answer, nil
		}
		out, err := fusee.Step(c, fmt.Sprintf("tool-%d-%s", turn, t.Tool), func(context.Context) (string, error) {
			return tools[t.Tool](t.Args), nil
		})
		if err != nil {
			return "", err
		}
		history = append(history, out)
	}
	return "", errors.New("no answer after 10 turns")
})

func main() {
	e, err := fusee.Open(fusee.Options{Dir: "./data/agent-loop", Workflows: []fusee.Definition{Agent}})
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	run, err := Agent.On(e).Start(ctx, "trip-question-1", "Do I need an umbrella in Lisbon?",
		fusee.StartOptions{IfRunning: fusee.IfRunningUseExisting, IfFinished: fusee.IfFinishedUseExisting})
	if err != nil {
		log.Fatal(err)
	}
	answer, err := run.Result(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(answer)
}
