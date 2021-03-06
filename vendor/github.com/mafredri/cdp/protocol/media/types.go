// Code generated by cdpgen. DO NOT EDIT.

package media

import (
	"encoding/json"
	"errors"
	"time"
)

// PlayerID Players will get an ID that is unique within the agent context.
type PlayerID string

// Timestamp
type Timestamp float64

// String calls (time.Time).String().
func (t Timestamp) String() string {
	return t.Time().String()
}

// Time parses the Unix time.
func (t Timestamp) Time() time.Time {
	ts := float64(t) / 1
	secs := int64(ts)
	nsecs := int64((ts - float64(secs)) * 1000000000)
	return time.Unix(secs, nsecs)
}

// MarshalJSON implements json.Marshaler. Encodes to null if t is zero.
func (t Timestamp) MarshalJSON() ([]byte, error) {
	if t == 0 {
		return []byte("null"), nil
	}
	f := float64(t)
	return json.Marshal(&f)
}

// UnmarshalJSON implements json.Unmarshaler.
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	*t = 0
	if len(data) == 0 {
		return nil
	}
	var f float64
	if err := json.Unmarshal(data, &f); err != nil {
		return errors.New("media.Timestamp: " + err.Error())
	}
	*t = Timestamp(f)
	return nil
}

var _ json.Marshaler = (*Timestamp)(nil)
var _ json.Unmarshaler = (*Timestamp)(nil)

// PlayerProperty Player Property type
type PlayerProperty struct {
	Name  string  `json:"name"`            // No description.
	Value *string `json:"value,omitempty"` // No description.
}

// PlayerEventType Break out events into different types
type PlayerEventType string

// PlayerEventType as enums.
const (
	PlayerEventTypeNotSet         PlayerEventType = ""
	PlayerEventTypeErrorEvent     PlayerEventType = "errorEvent"
	PlayerEventTypeTriggeredEvent PlayerEventType = "triggeredEvent"
	PlayerEventTypeMessageEvent   PlayerEventType = "messageEvent"
)

func (e PlayerEventType) Valid() bool {
	switch e {
	case "errorEvent", "triggeredEvent", "messageEvent":
		return true
	default:
		return false
	}
}

func (e PlayerEventType) String() string {
	return string(e)
}

// PlayerEvent
type PlayerEvent struct {
	Type      PlayerEventType `json:"type"`      // No description.
	Timestamp Timestamp       `json:"timestamp"` // Events are timestamped relative to the start of the player creation not relative to the start of playback.
	Name      string          `json:"name"`      // No description.
	Value     string          `json:"value"`     // No description.
}
