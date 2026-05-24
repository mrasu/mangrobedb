use crate::domain::partition::Partition;
use crate::domain::partition_range::{BoundInclusivity, ExpandedPartition, PartitionRange};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartitionFilter {
    pub predicates: Vec<PartitionPredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionPredicate {
    In(Vec<Partition>),
    Range(PartitionRange),
}

impl PartitionFilter {
    pub fn new_from_expanded_partition(expanded_partition: ExpandedPartition) -> Self {
        match expanded_partition {
            ExpandedPartition::Ranges(ranges) => PartitionFilter {
                predicates: ranges.into_iter().map(range_to_predicate).collect(),
            },
            ExpandedPartition::OpenStart { upper } => PartitionFilter {
                predicates: vec![PartitionPredicate::Range(PartitionRange {
                    lower: None,
                    upper: Some(upper),
                })],
            },
            ExpandedPartition::OpenEnd { lower } => PartitionFilter {
                predicates: vec![PartitionPredicate::Range(PartitionRange {
                    lower: Some(lower),
                    upper: None,
                })],
            },
            ExpandedPartition::FullyOpen => PartitionFilter::default(),
        }
    }
}

fn range_to_predicate(range: PartitionRange) -> PartitionPredicate {
    if let (Some(lower), Some(upper)) = (&range.lower, &range.upper)
        && lower.partition == upper.partition
        && lower.inclusivity == BoundInclusivity::Inclusive
        && upper.inclusivity == BoundInclusivity::Inclusive
    {
        return PartitionPredicate::In(vec![lower.partition]);
    }

    PartitionPredicate::Range(PartitionRange {
        lower: range.lower,
        upper: range.upper,
    })
}
