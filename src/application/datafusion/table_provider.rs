use std::any::Any;
use std::sync::Arc;

use crate::application::datafusion::column::INTERNAL_COLUMN_PREFIX;
use crate::application::datafusion::file_pruning::prune_files_by_statistics;
use crate::application::datafusion::partition::extract_partition_times;
use crate::domain::port::catalog::{CatalogFile, CatalogPort};
use crate::domain::table::Table;
use arrow::datatypes::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::project_schema;
use datafusion::datasource::TableProvider;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::DataFusionError;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use tracing::debug;
use vortex::VortexSessionDefault;
use vortex::session::VortexSession;
use vortex_datafusion::VortexFormat;

const DEFAULT_STREAM_ID: i64 = 0;
const GET_FILE_INFO_BATCH_SIZE: usize = 100;

#[derive(Debug)]
pub struct DummyTableProvider<C: CatalogPort> {
    table: Table,
    catalog_port: Arc<C>,
    schema: SchemaRef,
}

impl<C: CatalogPort> DummyTableProvider<C> {
    pub fn try_new(table: Table, catalog_port: Arc<C>) -> DataFusionResult<Self> {
        let schema = Arc::new(build_public_schema(&table)?);
        Ok(Self {
            schema,
            table,
            catalog_port,
        })
    }
}

#[async_trait]
impl<C: CatalogPort + 'static> TableProvider for DummyTableProvider<C> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let Some(partition_time_filter) = extract_partition_times(&self.table, filters)? else {
            return self.build_empty_plan(projection);
        };

        let files = self
            .catalog_port
            .get_current_state(
                &self.table.schema.table_name,
                DEFAULT_STREAM_ID,
                &partition_time_filter,
            )
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

        let pruned_files = self.prune_files(&files, filters).await?;
        let paths = resolve_catalog_paths(&self.table, &pruned_files);
        debug!(table_name = %self.table.schema.table_name, ?paths, "selected query files");
        if paths.is_empty() {
            return self.build_empty_plan(projection);
        }

        let table_paths = paths
            .iter()
            .map(ListingTableUrl::parse)
            .collect::<Result<Vec<_>, _>>()?;
        let format = Arc::new(VortexFormat::new(VortexSession::default()));
        let config = ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(ListingOptions::new(format))
            .with_schema(Arc::clone(&self.schema));
        let listing_table = ListingTable::try_new(config)?;

        listing_table.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }
}

impl<C: CatalogPort + 'static> DummyTableProvider<C> {
    fn build_empty_plan(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection)?;
        Ok(Arc::new(EmptyExec::new(projected_schema)))
    }

    async fn prune_files(
        &self,
        candidate_files: &[CatalogFile],
        filters: &[Expr],
    ) -> DataFusionResult<Vec<CatalogFile>> {
        let mut pruned_files = Vec::new();
        for files in candidate_files.chunks(GET_FILE_INFO_BATCH_SIZE) {
            pruned_files.extend(
                prune_files_by_statistics(
                    self.catalog_port.as_ref(),
                    &self.table.schema.table_name,
                    files,
                    filters,
                )
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
            );
        }

        Ok(pruned_files)
    }
}

fn resolve_catalog_paths(table: &Table, files: &[CatalogFile]) -> Vec<String> {
    files
        .iter()
        .map(|file| table.build_full_path(file))
        .collect()
}

fn build_public_schema(table: &Table) -> DataFusionResult<Schema> {
    let mut fields = Vec::with_capacity(table.schema.public_columns().len());
    for column in table.schema.public_columns() {
        if column.name.starts_with(INTERNAL_COLUMN_PREFIX) {
            return Err(DataFusionError::Plan(format!(
                "public schema contains an internal column: {}",
                column.name
            )));
        }

        fields.push(Field::new(&column.name, column.data_type().clone(), true));
    }

    Ok(Schema::new(fields))
}
