package calendar

import (
	"context"
	"fmt"
	"sync"
	"time"

	schedule "github.com/adrien19/chronoqueue/api/schedule/v1"
)

// MemoryBusinessCalendarProvider is an in-memory implementation of BusinessCalendarProvider
type MemoryBusinessCalendarProvider struct {
	calendars map[string]*schedule.BusinessCalendar
	mu        sync.RWMutex
}

// NewMemoryBusinessCalendarProvider creates a new in-memory business calendar provider
func NewMemoryBusinessCalendarProvider() *MemoryBusinessCalendarProvider {
	provider := &MemoryBusinessCalendarProvider{
		calendars: make(map[string]*schedule.BusinessCalendar),
	}

	// Add default business calendar
	defaultCalendar := &schedule.BusinessCalendar{
		CalendarId:  "default",
		Name:        "Default Business Calendar",
		Description: "Standard Monday-Friday business calendar",
		Holidays:    []*schedule.Holiday{},
		WeekendDays: []int32{6, 7}, // Saturday and Sunday
		Timezone:    "UTC",
	}
	provider.calendars["default"] = defaultCalendar

	return provider
}

// GetBusinessCalendar retrieves a business calendar by ID
func (p *MemoryBusinessCalendarProvider) GetBusinessCalendar(ctx context.Context, calendarID string) (*schedule.BusinessCalendar, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	calendar, exists := p.calendars[calendarID]
	if !exists {
		return nil, ErrBusinessCalendarNotFound.WithDetails(fmt.Sprintf("calendar ID: %s", calendarID))
	}

	// Return a copy to prevent modification
	return p.copyBusinessCalendar(calendar), nil
}

// CreateBusinessCalendar creates a new business calendar
func (p *MemoryBusinessCalendarProvider) CreateBusinessCalendar(ctx context.Context, calendar *schedule.BusinessCalendar) error {
	if calendar == nil {
		return fmt.Errorf("calendar cannot be nil")
	}

	if calendar.CalendarId == "" {
		return fmt.Errorf("calendar ID is required")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if calendar already exists
	if _, exists := p.calendars[calendar.CalendarId]; exists {
		return fmt.Errorf("calendar with ID %s already exists", calendar.CalendarId)
	}

	// Store a copy to prevent modification
	p.calendars[calendar.CalendarId] = p.copyBusinessCalendar(calendar)

	return nil
}

// UpdateBusinessCalendar updates an existing business calendar
func (p *MemoryBusinessCalendarProvider) UpdateBusinessCalendar(ctx context.Context, calendar *schedule.BusinessCalendar) error {
	if calendar == nil {
		return fmt.Errorf("calendar cannot be nil")
	}

	if calendar.CalendarId == "" {
		return fmt.Errorf("calendar ID is required")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if calendar exists
	if _, exists := p.calendars[calendar.CalendarId]; !exists {
		return ErrBusinessCalendarNotFound.WithDetails(fmt.Sprintf("calendar ID: %s", calendar.CalendarId))
	}

	// Store a copy to prevent modification
	p.calendars[calendar.CalendarId] = p.copyBusinessCalendar(calendar)

	return nil
}

// DeleteBusinessCalendar deletes a business calendar
func (p *MemoryBusinessCalendarProvider) DeleteBusinessCalendar(ctx context.Context, calendarID string) error {
	if calendarID == "" {
		return fmt.Errorf("calendar ID is required")
	}

	// Don't allow deletion of default calendar
	if calendarID == "default" {
		return fmt.Errorf("cannot delete default business calendar")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if calendar exists
	if _, exists := p.calendars[calendarID]; !exists {
		return ErrBusinessCalendarNotFound.WithDetails(fmt.Sprintf("calendar ID: %s", calendarID))
	}

	delete(p.calendars, calendarID)

	return nil
}

// ListBusinessCalendars lists all available business calendars
func (p *MemoryBusinessCalendarProvider) ListBusinessCalendars(ctx context.Context) ([]*schedule.BusinessCalendar, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	calendars := make([]*schedule.BusinessCalendar, 0, len(p.calendars))
	for _, calendar := range p.calendars {
		calendars = append(calendars, p.copyBusinessCalendar(calendar))
	}

	return calendars, nil
}

// copyBusinessCalendar creates a deep copy of a business calendar
func (p *MemoryBusinessCalendarProvider) copyBusinessCalendar(original *schedule.BusinessCalendar) *schedule.BusinessCalendar {
	if original == nil {
		return nil
	}

	copy := &schedule.BusinessCalendar{
		CalendarId:  original.CalendarId,
		Name:        original.Name,
		Description: original.Description,
		Timezone:    original.Timezone,
		WeekendDays: make([]int32, len(original.WeekendDays)),
		Holidays:    make([]*schedule.Holiday, len(original.Holidays)),
	}

	// Copy weekend days
	for i, day := range original.WeekendDays {
		copy.WeekendDays[i] = day
	}

	// Copy holidays
	for i, holiday := range original.Holidays {
		copy.Holidays[i] = p.copyHoliday(holiday)
	}

	return copy
}

// copyHoliday creates a deep copy of a holiday
func (p *MemoryBusinessCalendarProvider) copyHoliday(original *schedule.Holiday) *schedule.Holiday {
	if original == nil {
		return nil
	}

	copy := &schedule.Holiday{
		Name:            original.Name,
		Date:            original.Date,
		RecurringYearly: original.RecurringYearly,
		Rule:            original.Rule,
	}

	return copy
}

// Utility methods for creating common business calendars

// CreateUSBusinessCalendar creates a US business calendar with common federal holidays
func CreateUSBusinessCalendar() *schedule.BusinessCalendar {
	return &schedule.BusinessCalendar{
		CalendarId:  "us-business",
		Name:        "US Business Calendar",
		Description: "US business calendar with federal holidays",
		WeekendDays: []int32{6, 7}, // Saturday and Sunday
		Timezone:    "America/New_York",
		Holidays: []*schedule.Holiday{
			{
				Name:            "New Year's Day",
				RecurringYearly: true,
				Rule: &schedule.HolidayRule{
					Rule: &schedule.HolidayRule_Fixed{
						Fixed: &schedule.FixedDate{
							Month: 1,
							Day:   1,
						},
					},
				},
			},
			{
				Name:            "Independence Day",
				RecurringYearly: true,
				Rule: &schedule.HolidayRule{
					Rule: &schedule.HolidayRule_Fixed{
						Fixed: &schedule.FixedDate{
							Month: 7,
							Day:   4,
						},
					},
				},
			},
			{
				Name:            "Christmas Day",
				RecurringYearly: true,
				Rule: &schedule.HolidayRule{
					Rule: &schedule.HolidayRule_Fixed{
						Fixed: &schedule.FixedDate{
							Month: 12,
							Day:   25,
						},
					},
				},
			},
			{
				Name:            "Labor Day",
				RecurringYearly: true,
				Rule: &schedule.HolidayRule{
					Rule: &schedule.HolidayRule_Relative{
						Relative: &schedule.RelativeDate{
							Month:      9, // September
							Weekday:    1, // Monday
							Occurrence: 1, // First
						},
					},
				},
			},
			{
				Name:            "Thanksgiving",
				RecurringYearly: true,
				Rule: &schedule.HolidayRule{
					Rule: &schedule.HolidayRule_Relative{
						Relative: &schedule.RelativeDate{
							Month:      11, // November
							Weekday:    4,  // Thursday
							Occurrence: 4,  // Fourth
						},
					},
				},
			},
		},
	}
}

// CreateUKBusinessCalendar creates a UK business calendar with bank holidays
func CreateUKBusinessCalendar() *schedule.BusinessCalendar {
	return &schedule.BusinessCalendar{
		CalendarId:  "uk-business",
		Name:        "UK Business Calendar",
		Description: "UK business calendar with bank holidays",
		WeekendDays: []int32{6, 7}, // Saturday and Sunday
		Timezone:    "Europe/London",
		Holidays: []*schedule.Holiday{
			{
				Name:            "New Year's Day",
				RecurringYearly: true,
				Rule: &schedule.HolidayRule{
					Rule: &schedule.HolidayRule_Fixed{
						Fixed: &schedule.FixedDate{
							Month: 1,
							Day:   1,
						},
					},
				},
			},
			{
				Name:            "Christmas Day",
				RecurringYearly: true,
				Rule: &schedule.HolidayRule{
					Rule: &schedule.HolidayRule_Fixed{
						Fixed: &schedule.FixedDate{
							Month: 12,
							Day:   25,
						},
					},
				},
			},
			{
				Name:            "Boxing Day",
				RecurringYearly: true,
				Rule: &schedule.HolidayRule{
					Rule: &schedule.HolidayRule_Fixed{
						Fixed: &schedule.FixedDate{
							Month: 12,
							Day:   26,
						},
					},
				},
			},
		},
	}
}

// IsBusinessDay checks if a given date is a business day according to the calendar
func (p *MemoryBusinessCalendarProvider) IsBusinessDay(ctx context.Context, calendarID string, date time.Time) (bool, error) {
	calendar, err := p.GetBusinessCalendar(ctx, calendarID)
	if err != nil {
		return false, err
	}

	// Check if it's a weekend
	weekday := date.Weekday()
	for _, wd := range calendar.WeekendDays {
		if int32(weekday) == wd || (wd == 7 && weekday == time.Sunday) {
			return false, nil
		}
	}

	// Check if it's a holiday
	for _, holiday := range calendar.Holidays {
		holidayDate := holiday.Date.AsTime()
		if holidayDate.Year() == date.Year() && holidayDate.Month() == date.Month() && holidayDate.Day() == date.Day() {
			return false, nil
		}
	}

	return true, nil
}

// GetNextBusinessDay returns the next business day after the given date
func (p *MemoryBusinessCalendarProvider) GetNextBusinessDay(ctx context.Context, calendarID string, from time.Time) (time.Time, error) {
	current := from.AddDate(0, 0, 1) // Start from next day

	for i := 0; i < 365; i++ { // Maximum 1 year search
		isBusinessDay, err := p.IsBusinessDay(ctx, calendarID, current)
		if err != nil {
			return time.Time{}, err
		}
		if isBusinessDay {
			return current, nil
		}
		current = current.AddDate(0, 0, 1)
	}

	return time.Time{}, fmt.Errorf("no business day found within 1 year from %v", from)
}

// GetPreviousBusinessDay returns the previous business day before the given date
func (p *MemoryBusinessCalendarProvider) GetPreviousBusinessDay(ctx context.Context, calendarID string, from time.Time) (time.Time, error) {
	current := from.AddDate(0, 0, -1) // Start from previous day

	for i := 0; i < 365; i++ { // Maximum 1 year search
		isBusinessDay, err := p.IsBusinessDay(ctx, calendarID, current)
		if err != nil {
			return time.Time{}, err
		}
		if isBusinessDay {
			return current, nil
		}
		current = current.AddDate(0, 0, -1)
	}

	return time.Time{}, fmt.Errorf("no business day found within 1 year before %v", from)
}

// AddBusinessDays adds the specified number of business days to the given date
func (p *MemoryBusinessCalendarProvider) AddBusinessDays(ctx context.Context, calendarID string, from time.Time, days int) (time.Time, error) {
	if days == 0 {
		return from, nil
	}

	current := from
	remaining := days
	direction := 1

	if days < 0 {
		direction = -1
		remaining = -days
	}

	counted := 0
	for counted < remaining {
		current = current.AddDate(0, 0, direction)

		isBusinessDay, err := p.IsBusinessDay(ctx, calendarID, current)
		if err != nil {
			return time.Time{}, err
		}

		if isBusinessDay {
			counted++
		}

		// Safety check to prevent infinite loop
		if absInt(int(current.Sub(from).Hours()/24)) > 2000 {
			return time.Time{}, fmt.Errorf("exceeded maximum search range adding %d business days from %v", days, from)
		}
	}

	return current, nil
}

// absInt returns the absolute value of an integer
func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
