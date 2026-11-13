package fusee

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Payloads are recorded as JSON (encoding/json), except []byte, which is
// recorded as raw bytes after a zero byte, as in the other SDKs. A value of
// type json.RawMessage is recorded as the JSON it holds.

func encode(v any) ([]byte, error) {
	switch b := v.(type) {
	case nil:
		return []byte("null"), nil
	case []byte:
		out := make([]byte, len(b)+1)
		copy(out[1:], b)
		return out, nil
	case json.RawMessage:
		if len(b) == 0 {
			return []byte("null"), nil
		}
		return b, nil
	}
	return json.Marshal(v)
}

// decode reads a recorded payload into a T: an empty payload is T's zero
// value, raw bytes decode into []byte (or any), JSON into anything else.
func decode[T any](p []byte) (T, error) {
	var v T
	if len(p) == 0 {
		return v, nil
	}
	if p[0] == 0 {
		switch t := any(&v).(type) {
		case *[]byte:
			*t = append([]byte(nil), p[1:]...)
			return v, nil
		case *any:
			*t = append([]byte(nil), p[1:]...)
			return v, nil
		}
		return v, fmt.Errorf("a raw payload does not decode into %T", v)
	}
	if r, ok := any(&v).(*json.RawMessage); ok {
		*r = append(json.RawMessage(nil), p...)
		return v, nil
	}
	if err := json.Unmarshal(p, &v); err != nil {
		return v, fmt.Errorf("a recorded value does not decode into %T: %w", v, err)
	}
	return v, nil
}

// payloadText is a payload as text (error messages and reasons).
func payloadText(p []byte) string {
	if len(p) > 0 && p[0] == 0 {
		return string(p[1:])
	}
	return string(p)
}

// raw is a JSON payload as a json.RawMessage (raw bytes become a JSON
// string of their text).
func raw(p []byte) json.RawMessage {
	if len(p) == 0 {
		return json.RawMessage("null")
	}
	if p[0] == 0 {
		b, _ := json.Marshal(string(p[1:]))
		return b
	}
	return append(json.RawMessage(nil), p...)
}

func nowMs() uint64 { return uint64(time.Now().UnixMilli()) }

func ms(d time.Duration) uint64 {
	if d <= 0 {
		return 0
	}
	return uint64(d / time.Millisecond)
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func uuidText(b []byte) string {
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
