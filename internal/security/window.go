package security

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Window struct {
	From     *time.Time `json:"from,omitempty"`
	To       *time.Time `json:"to,omitempty"`
	Days     []string   `json:"days,omitempty"`
	Start    string     `json:"start,omitempty"`
	End      string     `json:"end,omitempty"`
	Timezone string     `json:"timezone,omitempty"`

	loc        *time.Location
	days       map[time.Weekday]bool
	start, end int
}

var weekdays = map[string]time.Weekday{
	"sun": time.Sunday, "mon": time.Monday, "tue": time.Tuesday,
	"wed": time.Wednesday, "thu": time.Thursday, "fri": time.Friday, "sat": time.Saturday,
}

func (w *Window) Compile() error {
	w.loc = time.UTC
	if w.Timezone != "" {
		loc, err := time.LoadLocation(w.Timezone)
		if err != nil {
			return fmt.Errorf("timezone %q: %w", w.Timezone, err)
		}
		w.loc = loc
	}
	if w.From != nil && w.To != nil && !w.To.After(*w.From) {
		return fmt.Errorf("to must be after from")
	}
	w.start, w.end = -1, -1
	if (w.Start == "") != (w.End == "") {
		return fmt.Errorf("start and end must be set together")
	}
	if w.Start != "" {
		var err error
		if w.start, err = clock(w.Start); err != nil {
			return err
		}
		if w.end, err = clock(w.End); err != nil {
			return err
		}
	}
	if len(w.Days) > 0 {
		w.days = map[time.Weekday]bool{}
		for _, day := range w.Days {
			name := strings.ToLower(strings.TrimSpace(day))
			if len(name) > 3 {
				name = name[:3]
			}
			weekday, ok := weekdays[name]
			if !ok {
				return fmt.Errorf("unknown day %q", day)
			}
			w.days[weekday] = true
		}
	}
	if w.From == nil && w.To == nil && w.start < 0 && len(w.days) == 0 {
		return fmt.Errorf("window needs from/to, days or start/end")
	}
	return nil
}

func clock(value string) (int, error) {
	hour, minute, ok := strings.Cut(strings.TrimSpace(value), ":")
	hh, hourErr := strconv.Atoi(hour)
	mm, minuteErr := strconv.Atoi(minute)
	if !ok || hourErr != nil || minuteErr != nil || hh < 0 || hh > 23 || mm < 0 || mm > 59 {
		return 0, fmt.Errorf("invalid time of day %q (use HH:MM)", value)
	}
	return hh*60 + mm, nil
}

func (w Window) Contains(value time.Time) bool {
	if w.From != nil && value.Before(*w.From) {
		return false
	}
	if w.To != nil && !value.Before(*w.To) {
		return false
	}
	loc := w.loc
	if loc == nil {
		loc = time.UTC
	}
	local := value.In(loc)
	if w.start >= 0 {
		minute := local.Hour()*60 + local.Minute()
		day := local.Weekday()
		var inside bool
		if w.start <= w.end {
			inside = minute >= w.start && minute < w.end
		} else {
			inside = minute >= w.start || minute < w.end
			if minute < w.end {
				day = (day + 6) % 7
			}
		}
		if !inside {
			return false
		}
		if len(w.days) > 0 && !w.days[day] {
			return false
		}
		return true
	}
	return len(w.days) == 0 || w.days[local.Weekday()]
}
