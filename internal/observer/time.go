package observer

import "time"

func monotonicNow() time.Time {
	return time.Now()
}

func monotonicSince(start time.Time) float64 {
	return time.Since(start).Seconds()
}
