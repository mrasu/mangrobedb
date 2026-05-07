use crate::domain::port::catalog::BoundInclusivity;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpandedPartitionTimes {
    Ranges(Vec<TimeRange>),
    OpenStart { end: TimeRangeBound },
    OpenEnd { start: TimeRangeBound },
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
            let range = self.ranges[0].clone();
            match (range.start, range.end) {
                (None, Some(end)) => {
                    return ExpandedPartitionTimes::OpenStart { end };
                }
                (Some(start), None) => {
                    return ExpandedPartitionTimes::OpenEnd { start };
                }
                (None, None) => {
                    return ExpandedPartitionTimes::FullyOpen;
                }
                (Some(_), Some(_)) => return ExpandedPartitionTimes::Ranges(vec![range]),
            }
        }

        if self
            .ranges
            .iter()
            .any(|range| range.start.is_none() || range.end.is_none())
        {
            return ExpandedPartitionTimes::FullyOpen;
        }

        ExpandedPartitionTimes::Ranges(self.ranges.clone())
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
    pub start: Option<TimeRangeBound>,
    pub end: Option<TimeRangeBound>,
}

impl TimeRange {
    pub fn new(start_hour_micros: Option<i64>, end_hour_micros: Option<i64>) -> Self {
        Self {
            start: start_hour_micros.map(TimeRangeBound::new_inclusive),
            end: end_hour_micros.map(TimeRangeBound::new_inclusive),
        }
    }

    pub fn new_lower(hour_micros: i64, inclusivity: BoundInclusivity) -> Self {
        Self {
            start: Some(TimeRangeBound {
                hour_micros,
                inclusivity,
            }),
            end: None,
        }
    }

    pub fn new_upper(hour_micros: i64, inclusivity: BoundInclusivity) -> Self {
        Self {
            start: None,
            end: Some(TimeRangeBound {
                hour_micros,
                inclusivity,
            }),
        }
    }

    pub fn new_full_open() -> Self {
        Self {
            start: None,
            end: None,
        }
    }

    pub fn convert_to_range_vec(self) -> TimeRangeVec {
        TimeRangeVec::new_from_ranges(vec![self])
    }

    fn is_valid(&self) -> bool {
        match (self.start, self.end) {
            (Some(start), Some(end)) => start.hour_micros <= end.hour_micros,
            _ => true,
        }
    }

    fn intersect(&self, other: &Self) -> Option<Self> {
        let range = Self {
            start: self.max_start(other),
            end: self.min_end(other),
        };

        range.is_valid().then_some(range)
    }

    fn union(&self, other: &Self) -> Self {
        Self {
            start: self.min_start(other),
            end: self.max_end(other),
        }
    }

    fn overlaps_or_touches(&self, other: &Self) -> bool {
        match (other.start, self.end) {
            (Some(right_start), Some(left_end))
                if right_start.hour_micros < left_end.hour_micros =>
            {
                true
            }
            (Some(right_start), Some(left_end))
                if right_start.hour_micros == left_end.hour_micros =>
            {
                right_start.inclusivity == BoundInclusivity::Inclusive
                    && left_end.inclusivity == BoundInclusivity::Inclusive
            }
            (Some(_), Some(_)) => false,
            _ => true,
        }
    }

    fn min_start(&self, other: &Self) -> Option<TimeRangeBound> {
        match (self.start, other.start) {
            (Some(left), Some(right)) => Some(min_bound(
                left,
                right,
                InclusivityPreference::PreferInclusive,
            )),
            _ => None,
        }
    }

    fn max_start(&self, other: &Self) -> Option<TimeRangeBound> {
        match (self.start, other.start) {
            (Some(left), Some(right)) => Some(max_bound(
                left,
                right,
                InclusivityPreference::PreferExclusive,
            )),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        }
    }

    fn min_end(&self, other: &Self) -> Option<TimeRangeBound> {
        match (self.end, other.end) {
            (Some(left), Some(right)) => Some(min_bound(
                left,
                right,
                InclusivityPreference::PreferExclusive,
            )),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        }
    }

    fn max_end(&self, other: &Self) -> Option<TimeRangeBound> {
        match (self.end, other.end) {
            (Some(left), Some(right)) => Some(max_bound(
                left,
                right,
                InclusivityPreference::PreferInclusive,
            )),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRangeBound {
    pub hour_micros: i64,
    pub inclusivity: BoundInclusivity,
}

#[derive(Debug, Clone, Copy)]
enum InclusivityPreference {
    PreferInclusive,
    PreferExclusive,
}

impl TimeRangeBound {
    fn new_inclusive(hour_micros: i64) -> Self {
        Self {
            hour_micros,
            inclusivity: BoundInclusivity::Inclusive,
        }
    }
}

fn min_bound(
    left: TimeRangeBound,
    right: TimeRangeBound,
    preference: InclusivityPreference,
) -> TimeRangeBound {
    match left.hour_micros.cmp(&right.hour_micros) {
        std::cmp::Ordering::Less => left,
        std::cmp::Ordering::Greater => right,
        std::cmp::Ordering::Equal => TimeRangeBound {
            hour_micros: left.hour_micros,
            inclusivity: prefer_inclusivity(left.inclusivity, right.inclusivity, preference),
        },
    }
}

fn max_bound(
    left: TimeRangeBound,
    right: TimeRangeBound,
    preference: InclusivityPreference,
) -> TimeRangeBound {
    match left.hour_micros.cmp(&right.hour_micros) {
        std::cmp::Ordering::Less => right,
        std::cmp::Ordering::Greater => left,
        std::cmp::Ordering::Equal => TimeRangeBound {
            hour_micros: left.hour_micros,
            inclusivity: prefer_inclusivity(left.inclusivity, right.inclusivity, preference),
        },
    }
}

fn prefer_inclusivity(
    left: BoundInclusivity,
    right: BoundInclusivity,
    preference: InclusivityPreference,
) -> BoundInclusivity {
    match preference {
        InclusivityPreference::PreferInclusive => {
            if left == BoundInclusivity::Inclusive || right == BoundInclusivity::Inclusive {
                BoundInclusivity::Inclusive
            } else {
                BoundInclusivity::Exclusive
            }
        }
        InclusivityPreference::PreferExclusive => {
            if left == BoundInclusivity::Exclusive || right == BoundInclusivity::Exclusive {
                BoundInclusivity::Exclusive
            } else {
                BoundInclusivity::Inclusive
            }
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

    ranges.sort_by_key(|range| {
        (
            range.start.map(|bound| bound.hour_micros),
            range.end.map(|bound| bound.hour_micros),
        )
    });

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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn bound(hour_micros: i64, inclusivity: BoundInclusivity) -> TimeRangeBound {
        TimeRangeBound {
            hour_micros,
            inclusivity,
        }
    }

    fn range(
        start: Option<(i64, BoundInclusivity)>,
        end: Option<(i64, BoundInclusivity)>,
    ) -> TimeRange {
        TimeRange {
            start: start.map(|(hour_micros, inclusivity)| bound(hour_micros, inclusivity)),
            end: end.map(|(hour_micros, inclusivity)| bound(hour_micros, inclusivity)),
        }
    }

    #[rstest]
    #[case::same_time_prefers_exclusive(
        range(Some((10, BoundInclusivity::Inclusive)), Some((20, BoundInclusivity::Inclusive))),
        range(Some((10, BoundInclusivity::Exclusive)), Some((20, BoundInclusivity::Exclusive))),
        Some(range(Some((10, BoundInclusivity::Exclusive)), Some((20, BoundInclusivity::Exclusive)))),
    )]
    #[case::right_overlaps_left_end(
        range(Some((10, BoundInclusivity::Inclusive)), Some((20, BoundInclusivity::Inclusive))),
        range(Some((5, BoundInclusivity::Inclusive)), Some((15, BoundInclusivity::Inclusive))),
        Some(range(Some((10, BoundInclusivity::Inclusive)), Some((15, BoundInclusivity::Inclusive)))),
    )]
    #[case::right_overlaps_left_start(
        range(Some((10, BoundInclusivity::Inclusive)), Some((20, BoundInclusivity::Inclusive))),
        range(Some((15, BoundInclusivity::Inclusive)), Some((25, BoundInclusivity::Inclusive))),
        Some(range(Some((15, BoundInclusivity::Inclusive)), Some((20, BoundInclusivity::Inclusive)))),
    )]
    fn intersects_ranges(
        #[case] left: TimeRange,
        #[case] right: TimeRange,
        #[case] expected: Option<TimeRange>,
    ) {
        assert_eq!(left.intersect(&right), expected);
    }

    #[rstest]
    #[case::same_time_prefers_inclusive(
        range(Some((10, BoundInclusivity::Exclusive)), Some((20, BoundInclusivity::Exclusive))),
        range(Some((10, BoundInclusivity::Inclusive)), Some((20, BoundInclusivity::Inclusive))),
        range(Some((10, BoundInclusivity::Inclusive)), Some((20, BoundInclusivity::Inclusive))),
    )]
    #[case::right_overlaps_left_end(
        range(Some((10, BoundInclusivity::Inclusive)), Some((20, BoundInclusivity::Inclusive))),
        range(Some((5, BoundInclusivity::Inclusive)), Some((15, BoundInclusivity::Inclusive))),
        range(Some((5, BoundInclusivity::Inclusive)), Some((20, BoundInclusivity::Inclusive))),
    )]
    #[case::right_overlaps_left_start(
        range(Some((10, BoundInclusivity::Inclusive)), Some((20, BoundInclusivity::Inclusive))),
        range(Some((15, BoundInclusivity::Inclusive)), Some((25, BoundInclusivity::Inclusive))),
        range(Some((10, BoundInclusivity::Inclusive)), Some((25, BoundInclusivity::Inclusive))),
    )]
    fn unions_ranges(
        #[case] left: TimeRange,
        #[case] right: TimeRange,
        #[case] expected: TimeRange,
    ) {
        assert_eq!(left.union(&right), expected);
    }

    #[rstest]
    #[case::both_include_touching_bound(
        range(Some((10, BoundInclusivity::Inclusive)), Some((20, BoundInclusivity::Inclusive))),
        range(Some((20, BoundInclusivity::Inclusive)), Some((30, BoundInclusivity::Inclusive))),
        true,
    )]
    #[case::left_excludes_touching_bound(
        range(Some((10, BoundInclusivity::Inclusive)), Some((20, BoundInclusivity::Exclusive))),
        range(Some((20, BoundInclusivity::Inclusive)), Some((30, BoundInclusivity::Inclusive))),
        false,
    )]
    #[case::right_excludes_touching_bound(
        range(Some((10, BoundInclusivity::Inclusive)), Some((20, BoundInclusivity::Inclusive))),
        range(Some((20, BoundInclusivity::Exclusive)), Some((30, BoundInclusivity::Inclusive))),
        false,
    )]
    #[case::both_exclude_touching_bound(
        range(Some((10, BoundInclusivity::Inclusive)), Some((20, BoundInclusivity::Exclusive))),
        range(Some((20, BoundInclusivity::Exclusive)), Some((30, BoundInclusivity::Inclusive))),
        false,
    )]
    fn checks_overlaps_or_touches(
        #[case] left: TimeRange,
        #[case] right: TimeRange,
        #[case] expected: bool,
    ) {
        assert_eq!(left.overlaps_or_touches(&right), expected);
    }

    #[rstest]
    #[case::single_closed_range(
        TimeRangeVec::new_from_ranges(vec![range(
            Some((10, BoundInclusivity::Inclusive)),
            Some((20, BoundInclusivity::Inclusive)),
        )]),
        ExpandedPartitionTimes::Ranges(vec![range(
            Some((10, BoundInclusivity::Inclusive)),
            Some((20, BoundInclusivity::Inclusive)),
        )]),
    )]
    #[case::open_start(
        TimeRangeVec::new_from_ranges(vec![range(
            None,
            Some((20, BoundInclusivity::Exclusive)),
        )]),
        ExpandedPartitionTimes::OpenStart {
            end: bound(20, BoundInclusivity::Exclusive),
        },
    )]
    #[case::open_end(
        TimeRangeVec::new_from_ranges(vec![range(
            Some((10, BoundInclusivity::Exclusive)),
            None,
        )]),
        ExpandedPartitionTimes::OpenEnd {
            start: bound(10, BoundInclusivity::Exclusive),
        },
    )]
    #[case::full_open(
        TimeRangeVec::new_from_ranges(vec![range(None, None)]),
        ExpandedPartitionTimes::FullyOpen,
    )]
    #[case::multiple_closed_ranges(
        TimeRangeVec::new_from_ranges(vec![
            range(
                Some((10, BoundInclusivity::Inclusive)),
                Some((20, BoundInclusivity::Inclusive)),
            ),
            range(
                Some((30, BoundInclusivity::Exclusive)),
                Some((40, BoundInclusivity::Exclusive)),
            ),
        ]),
        ExpandedPartitionTimes::Ranges(vec![
            range(
                Some((10, BoundInclusivity::Inclusive)),
                Some((20, BoundInclusivity::Inclusive)),
            ),
            range(
                Some((30, BoundInclusivity::Exclusive)),
                Some((40, BoundInclusivity::Exclusive)),
            ),
        ]),
    )]
    fn expands_partition_times(
        #[case] ranges: TimeRangeVec,
        #[case] expected: ExpandedPartitionTimes,
    ) {
        assert_eq!(ranges.to_expanded_partition_times(), expected);
    }
}
