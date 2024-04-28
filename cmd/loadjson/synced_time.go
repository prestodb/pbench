package loadjson

import (
	"sync"
	"time"
)

type syncedTime struct {
	t time.Time
	m sync.Mutex
}

func newSyncedTime(t time.Time) *syncedTime {
	return &syncedTime{
		t: t,
	}
}

func (st *syncedTime) Synchronized(f func(st *syncedTime)) {
	st.m.Lock()
	defer st.m.Unlock()
	f(st)
}

func (st *syncedTime) GetTime() time.Time {
	st.m.Lock()
	defer st.m.Unlock()
	return st.t
}
