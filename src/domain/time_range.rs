const HOUR_MICROS: i64 = 60 * 60 * 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpandedPartitionTimes {
    Bounded(Vec<i64>),
    OpenStart { end_hour_micros: i64 },
    OpenEnd { start_hour_micros: i64 },
    FullyOpen,
}

#[derive(Debug, Clone)]
pub struct TimeRangeVec {
    ranges: Vec<TimeRange>,
}

impl TimeRangeVec {
    pub fn new_from_ranges(ranges: Vec<TimeRange>) -> Self {
        Self { ranges }
    }

    pub fn new_full_open() -> Self {
        Self {
            ranges: vec![TimeRange::new_full_open()],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn to_expanded_partition_times(&self) -> ExpandedPartitionTimes {
        if self.ranges.len() == 1 {
            let range = &self.ranges[0];
            match (range.start_hour_micros, range.end_hour_micros) {
                (None, Some(end_hour_micros)) => {
                    return ExpandedPartitionTimes::OpenStart { end_hour_micros };
                }
                (Some(start_hour_micros), None) => {
                    return ExpandedPartitionTimes::OpenEnd { start_hour_micros };
                }
                (None, None) => {
                    return ExpandedPartitionTimes::FullyOpen;
                }
                (Some(_), Some(_)) => {}
            }
        }

        if self
            .ranges
            .iter()
            .any(|range| range.start_hour_micros.is_none() || range.end_hour_micros.is_none())
        {
            return ExpandedPartitionTimes::FullyOpen;
        }

        let mut partition_times = Vec::new();
        for range in &self.ranges {
            partition_times.extend(range.expand_hours());
        }

        ExpandedPartitionTimes::Bounded(partition_times)
    }

    pub fn union(&self, right: TimeRangeVec) -> TimeRangeVec {
        let mut ranges = self.ranges.clone();
        ranges.extend(right.ranges);
        merge_ranges(ranges)
    }

    fn intersect(&self, right: TimeRangeVec) -> TimeRangeVec {
        let mut intersections = Vec::new();

        for left_range in &self.ranges {
            for right_range in &right.ranges {
                if let Some(intersection) = left_range.intersect(right_range) {
                    intersections.push(intersection);
                }
            }
        }

        merge_ranges(intersections)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeRange {
    pub start_hour_micros: Option<i64>,
    pub end_hour_micros: Option<i64>,
}

impl TimeRange {
    pub fn new(start_hour_micros: Option<i64>, end_hour_micros: Option<i64>) -> Self {
        Self {
            start_hour_micros,
            end_hour_micros,
        }
    }

    pub fn new_full_open() -> Self {
        Self {
            start_hour_micros: None,
            end_hour_micros: None,
        }
    }

    pub fn convert_to_range_vec(self) -> TimeRangeVec {
        TimeRangeVec::new_from_ranges(vec![self])
    }

    fn is_valid(&self) -> bool {
        match (self.start_hour_micros, self.end_hour_micros) {
            (Some(start), Some(end)) => start <= end,
            _ => true,
        }
    }

    fn intersect(&self, other: &Self) -> Option<Self> {
        let range = Self {
            start_hour_micros: self.max_start(other),
            end_hour_micros: self.min_end(other),
        };

        range.is_valid().then_some(range)
    }

    fn union(&self, other: &Self) -> Self {
        Self {
            start_hour_micros: self.min_start(other),
            end_hour_micros: self.max_end(other),
        }
    }

    fn expand_hours(&self) -> Vec<i64> {
        let Some(mut current) = self.start_hour_micros else {
            return Vec::new();
        };
        let Some(end) = self.end_hour_micros else {
            return Vec::new();
        };

        let mut partition_times = Vec::new();
        while current <= end {
            partition_times.push(current);
            current += HOUR_MICROS;
        }

        partition_times
    }

    fn overlaps_or_touches(&self, other: &Self) -> bool {
        let left_end = self
            .end_hour_micros
            .map(|value| value.saturating_add(HOUR_MICROS));

        match (other.start_hour_micros, left_end) {
            (Some(right_start), Some(left_end)) => right_start <= left_end,
            _ => true,
        }
    }

    fn min_start(&self, other: &Self) -> Option<i64> {
        match (self.start_hour_micros, other.start_hour_micros) {
            (Some(left), Some(right)) => Some(left.min(right)),
            _ => None,
        }
    }

    fn max_start(&self, other: &Self) -> Option<i64> {
        match (self.start_hour_micros, other.start_hour_micros) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        }
    }

    fn min_end(&self, other: &Self) -> Option<i64> {
        match (self.end_hour_micros, other.end_hour_micros) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        }
    }

    fn max_end(&self, other: &Self) -> Option<i64> {
        match (self.end_hour_micros, other.end_hour_micros) {
            (Some(left), Some(right)) => Some(left.max(right)),
            _ => None,
        }
    }
}

pub fn intersect_optional_ranges(
    left: Option<TimeRangeVec>,
    right: Option<TimeRangeVec>,
) -> Option<TimeRangeVec> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.intersect(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn merge_ranges(mut ranges: Vec<TimeRange>) -> TimeRangeVec {
    if ranges.is_empty() {
        return TimeRangeVec::new_from_ranges(ranges);
    }

    ranges.sort_by_key(|range| (range.start_hour_micros, range.end_hour_micros));

    let mut merged: Vec<TimeRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        match merged.last_mut() {
            Some(last) if last.overlaps_or_touches(&range) => {
                *last = last.union(&range);
            }
            _ => merged.push(range),
        }
    }

    TimeRangeVec::new_from_ranges(merged)
}
