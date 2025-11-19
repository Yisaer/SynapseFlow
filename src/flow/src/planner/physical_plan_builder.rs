//! Physical plan builder - converts logical plans to physical plans

use crate::expr::sql_conversion::{
    convert_expr_to_scalar_with_bindings, SchemaBinding, SchemaBindingEntry,
};
use crate::planner::logical::{
    DataSource as LogicalDataSource, Filter as LogicalFilter, LogicalPlan,
    Project as LogicalProject,
};
use crate::planner::physical::physical_project::PhysicalProjectField;
use crate::planner::physical::{PhysicalDataSource, PhysicalFilter, PhysicalPlan, PhysicalProject};
use std::sync::Arc;

/// Create a physical plan from a logical plan
///
/// This function walks through the logical plan tree and creates corresponding physical plan nodes.
/// Uses downcast_ref for type-safe pattern matching and delegates to specific creation functions.
pub fn create_physical_plan(
    logical_plan: Arc<dyn LogicalPlan>,
    bindings: &SchemaBinding,
) -> Result<Arc<dyn PhysicalPlan>, String> {
    // Try to downcast to specific logical plan types and delegate to corresponding creation functions
    if let Some(logical_ds) = logical_plan.as_any().downcast_ref::<LogicalDataSource>() {
        create_physical_data_source(logical_ds, *logical_plan.get_plan_index(), bindings)
    } else if let Some(logical_filter) = logical_plan.as_any().downcast_ref::<LogicalFilter>() {
        create_physical_filter(
            logical_filter,
            &logical_plan,
            *logical_plan.get_plan_index(),
            bindings,
        )
    } else if let Some(logical_project) = logical_plan.as_any().downcast_ref::<LogicalProject>() {
        create_physical_project(
            logical_project,
            &logical_plan,
            *logical_plan.get_plan_index(),
            bindings,
        )
    } else {
        // Handle unsupported plan types
        Err(format!(
            "Unsupported logical plan type: {}",
            logical_plan.get_plan_type()
        ))
    }
}

/// Create a PhysicalDataSource from a LogicalDataSource
fn create_physical_data_source(
    logical_ds: &LogicalDataSource,
    index: i64,
    bindings: &SchemaBinding,
) -> Result<Arc<dyn PhysicalPlan>, String> {
    let entry = find_binding_entry(logical_ds, bindings)?;
    let physical_ds = PhysicalDataSource::new(
        logical_ds.source_name.clone(),
        logical_ds.alias.clone(),
        entry.schema.clone(),
        index,
    );
    Ok(Arc::new(physical_ds))
}

/// Create a PhysicalFilter from a LogicalFilter
fn create_physical_filter(
    logical_filter: &LogicalFilter,
    logical_plan: &Arc<dyn LogicalPlan>,
    index: i64,
    bindings: &SchemaBinding,
) -> Result<Arc<dyn PhysicalPlan>, String> {
    // Convert children first
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan(child.clone(), bindings)?;
        physical_children.push(physical_child);
    }

    // Convert SQL Expr to ScalarExpr
    let scalar_predicate =
        convert_expr_to_scalar_with_bindings(&logical_filter.predicate, bindings).map_err(|e| {
            format!(
                "Failed to convert filter predicate to scalar expression: {}",
                e
            )
        })?;

    let physical_filter = PhysicalFilter::new(
        logical_filter.predicate.clone(),
        scalar_predicate,
        physical_children,
        index,
    );
    Ok(Arc::new(physical_filter))
}

/// Create a PhysicalProject from a LogicalProject
fn create_physical_project(
    logical_project: &LogicalProject,
    logical_plan: &Arc<dyn LogicalPlan>,
    index: i64,
    bindings: &SchemaBinding,
) -> Result<Arc<dyn PhysicalPlan>, String> {
    // Convert children first
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan(child.clone(), bindings)?;
        physical_children.push(physical_child);
    }

    // Convert logical fields to physical fields
    let mut physical_fields = Vec::new();
    for logical_field in &logical_project.fields {
        let physical_field = PhysicalProjectField::from_logical(
            logical_field.field_name.clone(),
            logical_field.expr.clone(),
            bindings,
        )?;
        physical_fields.push(physical_field);
    }

    let physical_project = PhysicalProject::new(physical_fields, physical_children, index);
    Ok(Arc::new(physical_project))
}

fn find_binding_entry<'a>(
    logical_ds: &LogicalDataSource,
    bindings: &'a SchemaBinding,
) -> Result<&'a SchemaBindingEntry, String> {
    if let Some(alias) = logical_ds.alias.as_deref() {
        if let Some(entry) = bindings
            .entries()
            .iter()
            .find(|entry| entry.alias.as_ref().map(|a| a == alias).unwrap_or(false))
        {
            return Ok(entry);
        }
    }
    bindings
        .entries()
        .iter()
        .find(|entry| entry.source_name == logical_ds.source_name)
        .ok_or_else(|| {
            format!(
                "Schema binding not found for source {}",
                logical_ds.source_name
            )
        })
}
