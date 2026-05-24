use crate::domain::partition::Partition;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpandedPartition {
    Ranges(Vec<PartitionRange>),
    OpenStart { upper: PartitionRangeBound },
    OpenEnd { lower: PartitionRangeBound },
    FullyOpen,
}

#[derive(Debug, Clone)]
pub struct PartitionRangeVec {
    ranges: Vec<PartitionRange>,
}

impl PartitionRangeVec {
    pub fn new_from_ranges(ranges: Vec<PartitionRange>) -> Self {
        Self { ranges }
    }

    pub fn new_full_open() -> Self {
        Self {
            ranges: vec![PartitionRange::new_full_open()],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn to_expanded_partitions(&self) -> ExpandedPartition {
        if self.ranges.len() == 1 {
            let range = self.ranges[0].clone();
            return match (&range.lower, &range.upper) {
                (None, Some(upper)) => ExpandedPartition::OpenStart {
                    upper: upper.clone(),
                },
                (Some(lower), None) => ExpandedPartition::OpenEnd {
                    lower: lower.clone(),
                },
                (None, None) => ExpandedPartition::FullyOpen,
                (Some(_), Some(_)) => ExpandedPartition::Ranges(vec![range.clone()]),
            };
        }

        if self
            .ranges
            .iter()
            .any(|range| range.lower.is_none() || range.upper.is_none())
        {
            return ExpandedPartition::FullyOpen;
        }

        ExpandedPartition::Ranges(self.ranges.clone())
    }

    pub fn union(&self, right: PartitionRangeVec) -> PartitionRangeVec {
        let mut ranges = self.ranges.clone();
        ranges.extend(right.ranges);
        merge_ranges(ranges)
    }

    fn intersect(&self, right: PartitionRangeVec) -> PartitionRangeVec {
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
pub struct PartitionRange {
    pub lower: Option<PartitionRangeBound>,
    pub upper: Option<PartitionRangeBound>,
}

impl PartitionRange {
    pub fn new(lower: Option<Partition>, upper: Option<Partition>) -> Self {
        Self {
            lower: lower.map(PartitionRangeBound::new_inclusive),
            upper: upper.map(PartitionRangeBound::new_inclusive),
        }
    }

    pub fn new_lower(partition: Partition, inclusivity: BoundInclusivity) -> Self {
        Self {
            lower: Some(PartitionRangeBound {
                partition,
                inclusivity,
            }),
            upper: None,
        }
    }

    pub fn new_upper(partition: Partition, inclusivity: BoundInclusivity) -> Self {
        Self {
            lower: None,
            upper: Some(PartitionRangeBound {
                partition,
                inclusivity,
            }),
        }
    }

    pub fn new_full_open() -> Self {
        Self {
            lower: None,
            upper: None,
        }
    }

    pub fn convert_to_range_vec(self) -> PartitionRangeVec {
        PartitionRangeVec::new_from_ranges(vec![self])
    }

    fn is_valid(&self) -> bool {
        match (&self.lower, &self.upper) {
            (Some(lower), Some(upper)) => lower.partition <= upper.partition,
            _ => true,
        }
    }

    fn intersect(&self, other: &Self) -> Option<Self> {
        let range = Self {
            lower: self.max_lower(other),
            upper: self.min_upper(other),
        };

        range.is_valid().then_some(range)
    }

    fn union(&self, other: &Self) -> Self {
        Self {
            lower: self.min_lower(other),
            upper: self.max_upper(other),
        }
    }

    fn overlaps_or_touches(&self, other: &Self) -> bool {
        match (&other.lower, &self.upper) {
            (Some(right_lower), Some(left_upper))
                if right_lower.partition < left_upper.partition =>
            {
                true
            }
            (Some(right_lower), Some(left_upper))
                if right_lower.partition == left_upper.partition =>
            {
                right_lower.inclusivity == BoundInclusivity::Inclusive
                    && left_upper.inclusivity == BoundInclusivity::Inclusive
            }
            (Some(_), Some(_)) => false,
            _ => true,
        }
    }

    fn min_lower(&self, other: &Self) -> Option<PartitionRangeBound> {
        match (&self.lower, &other.lower) {
            (Some(left), Some(right)) => Some(min_bound(
                left,
                right,
                InclusivityPreference::PreferInclusive,
            )),
            _ => None,
        }
    }

    fn max_lower(&self, other: &Self) -> Option<PartitionRangeBound> {
        match (&self.lower, &other.lower) {
            (Some(left), Some(right)) => Some(max_bound(
                left,
                right,
                InclusivityPreference::PreferExclusive,
            )),
            (Some(left), None) => Some(left.clone()),
            (None, Some(right)) => Some(right.clone()),
            (None, None) => None,
        }
    }

    fn min_upper(&self, other: &Self) -> Option<PartitionRangeBound> {
        match (&self.upper, &other.upper) {
            (Some(left), Some(right)) => Some(min_bound(
                left,
                right,
                InclusivityPreference::PreferExclusive,
            )),
            (Some(left), None) => Some(left.clone()),
            (None, Some(right)) => Some(right.clone()),
            (None, None) => None,
        }
    }

    fn max_upper(&self, other: &Self) -> Option<PartitionRangeBound> {
        match (&self.upper, &other.upper) {
            (Some(left), Some(right)) => Some(max_bound(
                left,
                right,
                InclusivityPreference::PreferInclusive,
            )),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRangeBound {
    pub partition: Partition,
    pub inclusivity: BoundInclusivity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundInclusivity {
    Inclusive,
    Exclusive,
}

#[derive(Debug, Clone, Copy)]
enum InclusivityPreference {
    PreferInclusive,
    PreferExclusive,
}

impl PartitionRangeBound {
    fn new_inclusive(partition: Partition) -> Self {
        Self {
            partition,
            inclusivity: BoundInclusivity::Inclusive,
        }
    }
}

fn min_bound(
    left: &PartitionRangeBound,
    right: &PartitionRangeBound,
    preference: InclusivityPreference,
) -> PartitionRangeBound {
    match left.partition.cmp(&right.partition) {
        std::cmp::Ordering::Less => left.clone(),
        std::cmp::Ordering::Greater => right.clone(),
        std::cmp::Ordering::Equal => PartitionRangeBound {
            partition: left.partition,
            inclusivity: prefer_inclusivity(left.inclusivity, right.inclusivity, preference),
        },
    }
}

fn max_bound(
    left: &PartitionRangeBound,
    right: &PartitionRangeBound,
    preference: InclusivityPreference,
) -> PartitionRangeBound {
    match left.partition.cmp(&right.partition) {
        std::cmp::Ordering::Less => right.clone(),
        std::cmp::Ordering::Greater => left.clone(),
        std::cmp::Ordering::Equal => PartitionRangeBound {
            partition: left.partition,
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
    left: Option<PartitionRangeVec>,
    right: Option<PartitionRangeVec>,
) -> Option<PartitionRangeVec> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.intersect(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn merge_ranges(mut ranges: Vec<PartitionRange>) -> PartitionRangeVec {
    if ranges.is_empty() {
        return PartitionRangeVec::new_from_ranges(ranges);
    }

    ranges.sort_by_key(|range| {
        (
            range.lower.clone().map(|bound| bound.partition),
            range.upper.clone().map(|bound| bound.partition),
        )
    });

    let mut merged: Vec<PartitionRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        match merged.last_mut() {
            Some(last) if last.overlaps_or_touches(&range) => {
                *last = last.union(&range);
            }
            _ => merged.push(range),
        }
    }

    PartitionRangeVec::new_from_ranges(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn bound(partition: Partition, inclusivity: BoundInclusivity) -> PartitionRangeBound {
        PartitionRangeBound {
            partition,
            inclusivity,
        }
    }

    fn range(
        lower: Option<(i64, BoundInclusivity)>,
        upper: Option<(i64, BoundInclusivity)>,
    ) -> PartitionRange {
        PartitionRange {
            lower: lower.map(|(num, inclusivity)| bound(Partition::Int64(num), inclusivity)),
            upper: upper.map(|(num, inclusivity)| bound(Partition::Int64(num), inclusivity)),
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
        #[case] left: PartitionRange,
        #[case] right: PartitionRange,
        #[case] expected: Option<PartitionRange>,
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
        #[case] left: PartitionRange,
        #[case] right: PartitionRange,
        #[case] expected: PartitionRange,
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
        #[case] left: PartitionRange,
        #[case] right: PartitionRange,
        #[case] expected: bool,
    ) {
        assert_eq!(left.overlaps_or_touches(&right), expected);
    }

    #[rstest]
    #[case::single_closed_range(
        PartitionRangeVec::new_from_ranges(vec![range(
            Some((10, BoundInclusivity::Inclusive)),
            Some((20, BoundInclusivity::Inclusive)),
        )]),
        ExpandedPartition::Ranges(vec![range(
            Some((10, BoundInclusivity::Inclusive)),
            Some((20, BoundInclusivity::Inclusive)),
        )]),
    )]
    #[case::open_start(
        PartitionRangeVec::new_from_ranges(vec![range(
            None,
            Some((20, BoundInclusivity::Exclusive)),
        )]),
        ExpandedPartition::OpenStart {
            upper: bound(Partition::Int64(20), BoundInclusivity::Exclusive),
        },
    )]
    #[case::open_end(
        PartitionRangeVec::new_from_ranges(vec![range(
            Some((10, BoundInclusivity::Exclusive)),
            None,
        )]),
        ExpandedPartition::OpenEnd {
            lower: bound(Partition::Int64(10), BoundInclusivity::Exclusive),
        },
    )]
    #[case::full_open(
        PartitionRangeVec::new_from_ranges(vec![range(None, None)]),
        ExpandedPartition::FullyOpen,
    )]
    #[case::multiple_closed_ranges(
        PartitionRangeVec::new_from_ranges(vec![
            range(
                Some((10, BoundInclusivity::Inclusive)),
                Some((20, BoundInclusivity::Inclusive)),
            ),
            range(
                Some((30, BoundInclusivity::Exclusive)),
                Some((40, BoundInclusivity::Exclusive)),
            ),
        ]),
        ExpandedPartition::Ranges(vec![
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
    fn expands_partitions(#[case] ranges: PartitionRangeVec, #[case] expected: ExpandedPartition) {
        assert_eq!(ranges.to_expanded_partitions(), expected);
    }
}
