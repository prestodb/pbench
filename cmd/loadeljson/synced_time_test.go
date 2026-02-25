package loadeljson

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSyncedTime_GetTime(t *testing.T) {
	now := time.Now()
	st := newSyncedTime(now)

	retrieved := st.GetTime()
	assert.True(t, retrieved.Equal(now))
}

func TestSyncedTime_Synchronized(t *testing.T) {
	initialTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	st := newSyncedTime(initialTime)

	newTime := time.Date(2025, 6, 16, 8, 48, 8, 0, time.UTC)

	st.Synchronized(func(st *syncedTime) {
		st.t = newTime
	})

	retrieved := st.GetTime()
	assert.True(t, retrieved.Equal(newTime))
}

func TestSyncedTime_ConcurrentAccess(t *testing.T) {
	initialTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	st := newSyncedTime(initialTime)

	var wg sync.WaitGroup
	iterations := 100

	// Concurrent writes
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			newTime := initialTime.Add(time.Duration(idx) * time.Second)
			st.Synchronized(func(st *syncedTime) {
				// Simulate some work
				time.Sleep(time.Microsecond)
				st.t = newTime
			})
		}(i)
	}

	// Concurrent reads
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = st.GetTime()
		}()
	}

	wg.Wait()

	// Verify that the final time is one of the expected values
	finalTime := st.GetTime()
	assert.True(t, finalTime.After(initialTime) || finalTime.Equal(initialTime))
}

func TestSyncedTime_MinMaxTracking(t *testing.T) {
	// Simulate the use case in loadeljson where we track min start time and max end time
	minTime := newSyncedTime(time.Now())
	maxTime := newSyncedTime(time.UnixMilli(0))

	times := []time.Time{
		time.Date(2025, 6, 16, 8, 48, 8, 0, time.UTC),
		time.Date(2025, 6, 16, 8, 50, 0, 0, time.UTC),
		time.Date(2025, 6, 16, 8, 45, 0, 0, time.UTC),
		time.Date(2025, 6, 16, 9, 0, 0, 0, time.UTC),
	}

	for _, t := range times {
		// Update min time
		minTime.Synchronized(func(st *syncedTime) {
			if t.Before(st.t) {
				st.t = t
			}
		})

		// Update max time
		maxTime.Synchronized(func(st *syncedTime) {
			if t.After(st.t) {
				st.t = t
			}
		})
	}

	expectedMin := time.Date(2025, 6, 16, 8, 45, 0, 0, time.UTC)
	expectedMax := time.Date(2025, 6, 16, 9, 0, 0, 0, time.UTC)

	assert.True(t, minTime.GetTime().Equal(expectedMin))
	assert.True(t, maxTime.GetTime().Equal(expectedMax))
}
