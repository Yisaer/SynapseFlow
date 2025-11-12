//! Test for the new pipeline builder with downcast_ref approach

use flow::planner::physical::{PhysicalDataSource, PhysicalFilter};
use flow::processor::{build_processor_pipeline, execute_pipeline};
use tokio::sync::broadcast;

async fn test_single_data_source_pipeline() {
    // Test with just a DataSource (simplest case)
    let data_source = std::sync::Arc::new(PhysicalDataSource::new("simple_source".to_string(), 1));
    
    let result = build_processor_pipeline(data_source.clone());
    assert!(result.is_ok(), "Failed to build single data source pipeline: {:?}", result.err());
    
    let _pipeline_node = result.unwrap();
    println!("Successfully built single DataSource pipeline");
    
    // Test that we can execute it
    let execution_result = execute_pipeline(data_source);
    assert!(execution_result.is_ok(), "Failed to execute single data source: {:?}", execution_result.err());
    
    let processor_view = execution_result.unwrap();
    
    // Try to receive some data (should get stream start signal at minimum)
    let mut result_receiver = processor_view.result_resubscribe();
    
    // Give the processor a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Try to receive data - should at least get stream start
    match result_receiver.try_recv() {
        Ok(Ok(stream_data)) => {
            println!("Received stream data: {:?}", stream_data.description());
            assert!(stream_data.is_control(), "Expected control signal");
        }
        Ok(Err(e)) => {
            println!("Received error: {}", e);
        }
        Err(broadcast::error::TryRecvError::Empty) => {
            println!("No data available yet - this is expected");
        }
        Err(broadcast::error::TryRecvError::Closed) => {
            println!("Channel closed - this might indicate an issue");
        }
        Err(broadcast::error::TryRecvError::Lagged(_)) => {
            println!("Channel lagged - some messages were dropped");
        }
    }
    
    // Clean shutdown
    let _ = processor_view.stop();
    
    println!("Single DataSource test completed successfully");
}

async fn test_simple_filter_pipeline() {
    // Test with DataSource -> Filter pipeline
    use sqlparser::ast::{Expr, Value};
    
    let data_source = std::sync::Arc::new(PhysicalDataSource::new("filter_test_source".to_string(), 1));
    
    let filter_expr = Expr::Value(Value::Boolean(true)); // Always true for testing
    let filter = std::sync::Arc::new(PhysicalFilter::with_single_child(
        filter_expr,
        data_source.clone(),
        2,
    ));
    
    let result = build_processor_pipeline(filter.clone());
    assert!(result.is_ok(), "Failed to build filter pipeline: {:?}", result.err());
    
    let _pipeline_node = result.unwrap();
    println!("Successfully built Filter pipeline");
    
    // Test execution
    let execution_result = execute_pipeline(filter);
    assert!(execution_result.is_ok(), "Failed to execute filter pipeline: {:?}", execution_result.err());
    
    let processor_view = execution_result.unwrap();
    println!("Successfully started filter pipeline execution");
    
    // Clean shutdown
    let _ = processor_view.stop();
    
    println!("Filter pipeline test completed successfully");
}

async fn test_unsupported_plan_type() {
    // Create a mock unsupported plan - we'll use a simple wrapper
    #[derive(Debug)]
    struct UnsupportedPlan {
        base: flow::planner::physical::BasePhysicalPlan,
    }
    
    impl flow::planner::physical::PhysicalPlan for UnsupportedPlan {
        fn children(&self) -> &[std::sync::Arc<dyn flow::planner::physical::PhysicalPlan>] {
            &self.base.children
        }
        
        fn get_plan_type(&self) -> &str {
            "UnsupportedPlan"
        }
        
        fn get_plan_index(&self) -> &i64 {
            &self.base.index
        }
        
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }
    
    let unsupported = std::sync::Arc::new(UnsupportedPlan {
        base: flow::planner::physical::BasePhysicalPlan::new_leaf(999),
    });
    
    let result = build_processor_pipeline(unsupported);
    assert!(result.is_err(), "Expected unsupported plan to fail");
    
    let error_msg = result.unwrap_err();
    println!("Got expected error for unsupported plan: {}", error_msg);
    assert!(error_msg.contains("Unsupported physical plan type"));
}

#[tokio::main]
async fn main() {
    println!("Running pipeline builder tests...");
    
    println!("\n=== Testing single DataSource ===");
    test_single_data_source_pipeline().await;
    
    println!("\n=== Testing Filter pipeline ===");
    test_simple_filter_pipeline().await;
    
    println!("\n=== Testing unsupported plan ===");
    test_unsupported_plan_type().await;
    
    println!("\n✅ All tests completed!");
}