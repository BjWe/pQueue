package pQueue;

import java.time.LocalTime;

class TimeRange {
    private final LocalTime start;
    private final LocalTime end;

    public TimeRange(LocalTime start, LocalTime end) {
        this.start = start;
        this.end = end;
    }

    public boolean isWithinRange(LocalTime time) {
        return !time.isBefore(start) && !time.isAfter(end);
    }
}