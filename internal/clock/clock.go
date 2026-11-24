package clock

import (
	"sync"
	"time"
)

// Clock isolates wall-clock reads so scheduling and retention logic can be
// tested with a deterministic fake.
type Clock interface {
	Now() time.Time
}

type Real struct{}

func (Real) Now() time.Time { return time.Now() }

type Fake struct {
	mu  sync.Mutex
	now time.Time
}

func NewFake(t time.Time) *Fake { return &Fake{now: t} }

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t
}

func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// YearIn returns the calendar year of t in loc. Annual shard attribution must
// always use this rather than the host timezone.
func YearIn(t time.Time, loc *time.Location) int {
	if loc == nil {
		loc = time.UTC
	}
	return t.In(loc).Year()
}
