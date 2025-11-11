#[cfg(test)]
mod tests {
    use crate::planner::physical::{PhysicalPlan, PhysicalDataSource, PhysicalProject, PhysicalFilter};
    use std::sync::Arc;
    
    #[test]
    fn test_physical_data_source_creation() {
        let data_source = PhysicalDataSource::new("test_source".to_string(), 1);
        
        assert_eq!(data_source.get_plan_type(), "PhysicalDataSource");
        assert_eq!(data_source.get_plan_index(), &1);
        assert_eq!(data_source.source_name, "test_source");
        assert!(data_source.children().is_empty());
    }
    
    #[test]
    fn test_physical_project_creation() {
        let data_source = Arc::new(PhysicalDataSource::new("test_source".to_string(), 1));
        let fields = vec![];
        let project = PhysicalProject::with_single_child(fields, data_source.clone(), 2);
        
        assert_eq!(project.get_plan_type(), "PhysicalProject");
        assert_eq!(project.get_plan_index(), &2);
        assert_eq!(project.children().len(), 1);
    }
    
    #[test]
    fn test_physical_filter_creation() {
        let data_source = Arc::new(PhysicalDataSource::new("test_source".to_string(), 1));
        let predicate = sqlparser::ast::Expr::Value(sqlparser::ast::Value::Boolean(true));
        let filter = PhysicalFilter::with_single_child(predicate, data_source.clone(), 2);
        
        assert_eq!(filter.get_plan_type(), "PhysicalFilter");
        assert_eq!(filter.get_plan_index(), &2);
        assert_eq!(filter.children().len(), 1);
    }
    
    #[test]
    fn test_physical_plan_hierarchy() {
        // Create a simple plan hierarchy: DataSource -> Filter -> Project
        let data_source = Arc::new(PhysicalDataSource::new("test_source".to_string(), 1));
        
        let predicate = sqlparser::ast::Expr::Value(sqlparser::ast::Value::Boolean(true));
        let filter = Arc::new(PhysicalFilter::with_single_child(predicate, data_source.clone(), 2));
        
        let fields = vec![];
        let project = PhysicalProject::with_single_child(fields, filter.clone(), 3);
        
        // Test the hierarchy
        assert_eq!(project.children().len(), 1);
        assert_eq!(project.children()[0].get_plan_index(), &2);
        
        assert_eq!(filter.children().len(), 1);
        assert_eq!(filter.children()[0].get_plan_index(), &1);
        
        assert_eq!(data_source.children().len(), 0);
    }
}