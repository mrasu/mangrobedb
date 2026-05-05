pub const INTERNAL_COLUMN_PREFIX: &str = "__mangrobe__";

pub fn to_internal_column_name(col_name: &str) -> String {
    format!("{INTERNAL_COLUMN_PREFIX}_{col_name}")
}
