package calendar

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/adrien19/chronoqueue/pkg/calendar/types"
)

// DefaultTimezoneProvider is the default implementation of TimezoneProvider
type DefaultTimezoneProvider struct {
	// Cache of loaded timezones to avoid repeated loading
	timezoneCache map[string]*time.Location
}

// NewDefaultTimezoneProvider creates a new default timezone provider
func NewDefaultTimezoneProvider() *DefaultTimezoneProvider {
	return &DefaultTimezoneProvider{
		timezoneCache: make(map[string]*time.Location),
	}
}

// GetTimezone returns a timezone by name
func (p *DefaultTimezoneProvider) GetTimezone(ctx context.Context, name string) (*time.Location, error) {
	if name == "" {
		return time.UTC, nil
	}

	// Check cache first
	if loc, exists := p.timezoneCache[name]; exists {
		return loc, nil
	}

	// Load timezone
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, ErrInvalidTimezone.WithDetails(fmt.Sprintf("failed to load timezone %s: %v", name, err))
	}

	// Cache the result
	p.timezoneCache[name] = loc

	return loc, nil
}

// ListTimezones returns available timezones
func (p *DefaultTimezoneProvider) ListTimezones(ctx context.Context) ([]string, error) {
	// Return a curated list of common timezones
	// In a full implementation, this might read from the system's timezone database
	timezones := []string{
		// UTC
		"UTC",

		// Americas
		"America/New_York",
		"America/Chicago",
		"America/Denver",
		"America/Los_Angeles",
		"America/Toronto",
		"America/Vancouver",
		"America/Mexico_City",
		"America/Sao_Paulo",
		"America/Argentina/Buenos_Aires",

		// Europe
		"Europe/London",
		"Europe/Paris",
		"Europe/Berlin",
		"Europe/Rome",
		"Europe/Madrid",
		"Europe/Amsterdam",
		"Europe/Brussels",
		"Europe/Zurich",
		"Europe/Vienna",
		"Europe/Prague",
		"Europe/Warsaw",
		"Europe/Stockholm",
		"Europe/Helsinki",
		"Europe/Oslo",
		"Europe/Copenhagen",
		"Europe/Dublin",
		"Europe/Lisbon",
		"Europe/Athens",
		"Europe/Istanbul",
		"Europe/Moscow",

		// Asia
		"Asia/Tokyo",
		"Asia/Shanghai",
		"Asia/Hong_Kong",
		"Asia/Singapore",
		"Asia/Seoul",
		"Asia/Taipei",
		"Asia/Bangkok",
		"Asia/Jakarta",
		"Asia/Manila",
		"Asia/Kuala_Lumpur",
		"Asia/Ho_Chi_Minh",
		"Asia/Kolkata",
		"Asia/Mumbai",
		"Asia/Karachi",
		"Asia/Dubai",
		"Asia/Riyadh",
		"Asia/Tehran",
		"Asia/Baghdad",
		"Asia/Jerusalem",

		// Australia/Pacific
		"Australia/Sydney",
		"Australia/Melbourne",
		"Australia/Brisbane",
		"Australia/Perth",
		"Australia/Adelaide",
		"Pacific/Auckland",
		"Pacific/Fiji",
		"Pacific/Honolulu",

		// Africa
		"Africa/Cairo",
		"Africa/Lagos",
		"Africa/Johannesburg",
		"Africa/Nairobi",
		"Africa/Casablanca",
	}

	sort.Strings(timezones)
	return timezones, nil
}

// ValidateTimezone checks if a timezone name is valid
func (p *DefaultTimezoneProvider) ValidateTimezone(ctx context.Context, name string) error {
	if name == "" {
		return nil // Empty timezone is valid (defaults to UTC)
	}

	// Try to load the timezone
	_, err := time.LoadLocation(name)
	if err != nil {
		return ErrInvalidTimezone.WithDetails(fmt.Sprintf("invalid timezone %s: %v", name, err))
	}

	return nil
}

// GetTimezoneInfo returns detailed information about a timezone at a specific time
func (p *DefaultTimezoneProvider) GetTimezoneInfo(ctx context.Context, name string, at time.Time) (*types.TimezoneInfo, error) {
	loc, err := p.GetTimezone(ctx, name)
	if err != nil {
		return nil, err
	}

	timeInTZ := at.In(loc)
	_, offset := timeInTZ.Zone()

	info := &types.TimezoneInfo{
		Name:         name,
		Abbreviation: timeInTZ.Format("MST"),
		Offset:       time.Duration(offset) * time.Second,
		IsDST:        p.isDST(timeInTZ),
	}

	// Find DST transitions if applicable
	if info.IsDST || p.hasDST(loc) {
		dstStart, dstEnd := p.findDSTTransitions(at, loc)
		if !dstStart.IsZero() {
			info.DSTStart = &dstStart
		}
		if !dstEnd.IsZero() {
			info.DSTEnd = &dstEnd
		}
	}

	return info, nil
}

// GetCommonTimezones returns a list of commonly used timezones organized by region
func (p *DefaultTimezoneProvider) GetCommonTimezones(ctx context.Context) (map[string][]string, error) {
	return map[string][]string{
		"UTC": {
			"UTC",
		},
		"Americas": {
			"America/New_York",
			"America/Chicago",
			"America/Denver",
			"America/Los_Angeles",
			"America/Toronto",
			"America/Mexico_City",
			"America/Sao_Paulo",
		},
		"Europe": {
			"Europe/London",
			"Europe/Paris",
			"Europe/Berlin",
			"Europe/Rome",
			"Europe/Madrid",
			"Europe/Amsterdam",
			"Europe/Moscow",
		},
		"Asia": {
			"Asia/Tokyo",
			"Asia/Shanghai",
			"Asia/Hong_Kong",
			"Asia/Singapore",
			"Asia/Seoul",
			"Asia/Kolkata",
			"Asia/Dubai",
		},
		"Australia": {
			"Australia/Sydney",
			"Australia/Melbourne",
			"Australia/Brisbane",
			"Australia/Perth",
		},
		"Africa": {
			"Africa/Cairo",
			"Africa/Lagos",
			"Africa/Johannesburg",
			"Africa/Nairobi",
		},
	}, nil
}

// Helper methods

func (p *DefaultTimezoneProvider) getDisplayName(name string) string {
	// Convert timezone name to a more readable format
	parts := strings.Split(name, "/")
	if len(parts) == 2 {
		city := strings.ReplaceAll(parts[1], "_", " ")
		return fmt.Sprintf("%s (%s)", city, parts[0])
	}
	return name
}

func (p *DefaultTimezoneProvider) getOffsetSeconds(t time.Time) int {
	_, offset := t.Zone()
	return offset
}

func (p *DefaultTimezoneProvider) isDST(t time.Time) bool {
	// Check if the current time is in daylight saving time
	// This is a simplified check; a full implementation would be more robust
	name, _ := t.Zone()

	// Common DST abbreviations
	dstAbbreviations := []string{"PDT", "MDT", "CDT", "EDT", "BST", "CEST", "AEST"}

	for _, abbr := range dstAbbreviations {
		if name == abbr {
			return true
		}
	}

	return false
}

// TimezoneInfo contains detailed information about a timezone
type TimezoneInfo struct {
	Name          string         `json:"name"`
	DisplayName   string         `json:"display_name"`
	Abbreviation  string         `json:"abbreviation"`
	Offset        string         `json:"offset"`         // e.g., "-0500"
	OffsetSeconds int            `json:"offset_seconds"` // seconds from UTC
	IsDST         bool           `json:"is_dst"`
	Location      *time.Location `json:"-"` // Don't serialize this
}

// ConvertTime converts a time from one timezone to another
func (p *DefaultTimezoneProvider) ConvertTime(ctx context.Context, t time.Time, fromTZ, toTZ string) (time.Time, error) {
	fromLoc, err := p.GetTimezone(ctx, fromTZ)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid source timezone: %w", err)
	}

	toLoc, err := p.GetTimezone(ctx, toTZ)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid target timezone: %w", err)
	}

	// Convert to the target timezone
	timeInFrom := t.In(fromLoc)
	timeInTo := timeInFrom.In(toLoc)

	return timeInTo, nil
}

// GetDSTTransitions returns the DST transitions for a timezone in a given year
func (p *DefaultTimezoneProvider) GetDSTTransitions(ctx context.Context, timezone string, year int) ([]DSTTransition, error) {
	loc, err := p.GetTimezone(ctx, timezone)
	if err != nil {
		return nil, err
	}

	var transitions []DSTTransition

	// Check each month for potential DST transitions
	// This is a simplified approach; a full implementation would use timezone database
	for month := 1; month <= 12; month++ {
		start := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, loc)
		end := start.AddDate(0, 1, -1) // Last day of month

		startDST := p.isDST(start)
		endDST := p.isDST(end)

		if startDST != endDST {
			// Find the exact transition date by binary search
			transitionDate := p.findTransitionDate(start, end)

			transition := DSTTransition{
				Date:         transitionDate,
				IsStart:      !startDST && endDST,
				OffsetBefore: p.getOffsetSeconds(start),
				OffsetAfter:  p.getOffsetSeconds(end),
			}

			transitions = append(transitions, transition)
		}
	}

	return transitions, nil
}

// findTransitionDate finds the exact DST transition date between two dates
func (p *DefaultTimezoneProvider) findTransitionDate(start, end time.Time) time.Time {
	// Binary search for the transition date
	for end.Sub(start) > 24*time.Hour {
		mid := start.Add(end.Sub(start) / 2)

		if p.isDST(start) == p.isDST(mid) {
			start = mid
		} else {
			end = mid
		}
	}

	return end
}

// DSTTransition represents a daylight saving time transition
type DSTTransition struct {
	Date         time.Time `json:"date"`
	IsStart      bool      `json:"is_start"`      // true for start of DST, false for end
	OffsetBefore int       `json:"offset_before"` // offset in seconds before transition
	OffsetAfter  int       `json:"offset_after"`  // offset in seconds after transition
}

// hasDST checks if a timezone has daylight saving time transitions
func (p *DefaultTimezoneProvider) hasDST(loc *time.Location) bool {
	// Check if there's a DST transition in the current year
	now := time.Now()
	start := time.Date(now.Year(), 1, 1, 0, 0, 0, 0, loc)
	end := time.Date(now.Year(), 12, 31, 23, 59, 59, 0, loc)

	_, offsetStart := start.Zone()
	_, offsetEnd := end.Zone()

	return offsetStart != offsetEnd
}

// findDSTTransitions finds the DST start and end times for the year containing the given time
func (p *DefaultTimezoneProvider) findDSTTransitions(at time.Time, loc *time.Location) (time.Time, time.Time) {
	var dstStart, dstEnd time.Time

	// Search for transitions throughout the year
	year := at.Year()
	start := time.Date(year, 1, 1, 0, 0, 0, 0, loc)

	var lastOffset int
	_, lastOffset = start.Zone()

	// Check each day for offset changes
	for d := 0; d < 366; d++ {
		current := start.AddDate(0, 0, d)
		if current.Year() != year {
			break
		}

		_, currentOffset := current.Zone()

		if currentOffset != lastOffset {
			if currentOffset > lastOffset {
				// DST started
				dstStart = current
			} else {
				// DST ended
				dstEnd = current
			}
			lastOffset = currentOffset
		}
	}

	return dstStart, dstEnd
}
