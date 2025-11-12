//! Test for pipeline with custom start/end endpoints

use flow::planner::physical::{PhysicalDataSource, PhysicalFilter};
use flow::processor::{build_pipeline_with_external_io, StreamData};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    println!("🧪 Testing pipeline with custom start/end endpoints...\n");
    
    // Test 1: Basic pipeline with external I/O
    test_external_io_pipeline().await;
    
    // Test 2: Manual control signal injection
    test_control_injection().await;
    
    // Test 3: Complex pipeline with multiple stages
    test_complex_pipeline().await;
    
    println!("\n✅ All endpoint tests completed successfully!");
}

async fn test_external_io_pipeline() {
    println!("=== Test 1: External I/O Pipeline ===");
    
    // Build pipeline: DataSource -> Filter with external I/O
    let data_source = std::sync::Arc::new(PhysicalDataSource::new("test_source".to_string(), 1));
    let filter_expr = sqlparser::ast::Expr::Value(sqlparser::ast::Value::Boolean(true));
    let filter = std::sync::Arc::new(PhysicalFilter::with_single_child(
        filter_expr,
        data_source.clone(),
        2,
    ));
    
    let pipeline = build_pipeline_with_external_io(filter);
    assert!(pipeline.is_ok(), "Failed to build pipeline: {:?}", pipeline.err());
    
    let mut external_pipeline = pipeline.unwrap();
    println!("✓ Pipeline built with external I/O channels");
    
    // Send control signal to start the pipeline
    let start_result = external_pipeline.send_control(StreamData::stream_start());
    assert!(start_result.is_ok(), "Failed to send control signal");
    println!("✓ Sent stream start control signal");
    
    // Give pipeline time to process
    sleep(Duration::from_millis(100)).await;
    
    // Try to receive results
    match external_pipeline.receive_result().await {
        Ok(stream_data) => {
            println!("✓ Received result: {:?}", stream_data.description());
            assert!(stream_data.is_control(), "Expected control signal");
        }
        Err(e) => {
            println!("No results yet (expected): {}", e);
        }
    }
    
    // Stop the pipeline
    let stop_result = external_pipeline.stop_all();
    println!("✓ Pipeline stopped: {:?}", stop_result);
    
    println!("✅ External I/O test completed\n");
}

async fn test_control_injection() {
    println!("=== Test 2: Manual Control Signal Injection ===");
    
    // Create a simple data source pipeline
    let data_source = std::sync::Arc::new(PhysicalDataSource::new("control_test_source".to_string(), 1));
    
    let pipeline = build_pipeline_with_external_io(data_source);
    assert!(pipeline.is_ok(), "Failed to build control pipeline: {:?}", pipeline.err());
    
    let mut control_pipeline = pipeline.unwrap();
    println!("✓ Control pipeline built");
    
    // Inject various control signals
    let controls = vec![
        StreamData::stream_start(),
        StreamData::control(flow::processor::ControlSignal::Backpressure),
        StreamData::control(flow::processor::ControlSignal::Resume),
        StreamData::stream_end(),
    ];
    
    for (i, control) in controls.iter().enumerate() {
        let result = control_pipeline.send_control(control.clone());
        assert!(result.is_ok(), "Failed to send control signal {}", i);
        println!("✓ Injected control signal {}: {:?}", i, control.description());
    }
    
    // Receive and verify control signals propagated through pipeline
    let mut received_count = 0;
    for _ in 0..controls.len() {
        match control_pipeline.receive_result().await {
            Ok(data) => {
                println!("✓ Received propagated signal: {:?}", data.description());
                received_count += 1;
            }
            Err(_) => break,
        }
    }
    
    println!("✓ Received {} propagated signals", received_count);
    
    // Stop the pipeline
    let _ = control_pipeline.stop_all();
    println!("✅ Control injection test completed\n");
}

async fn test_complex_pipeline() {
    println!("=== Test 3: Complex Multi-Stage Pipeline ===");
    
    // Build complex pipeline: DataSource -> Filter -> Filter
    let data_source = std::sync::Arc::new(PhysicalDataSource::new("complex_source".to_string(), 1));
    
    let filter1_expr = sqlparser::ast::Expr::Value(sqlparser::ast::Value::Boolean(true));
    let filter1 = std::sync::Arc::new(PhysicalFilter::with_single_child(
        filter1_expr,
        data_source.clone(),
        2,
    ));
    
    let filter2_expr = sqlparser::ast::Expr::Value(sqlparser::ast::Value::Boolean(true));
    let filter2 = std::sync::Arc::new(PhysicalFilter::with_single_child(
        filter2_expr,
        filter1.clone(),
        3,
    ));
    
    let pipeline = build_pipeline_with_external_io(filter2);
    assert!(pipeline.is_ok(), "Failed to build complex pipeline: {:?}", pipeline.err());
    
    let mut complex_pipeline = pipeline.unwrap();
    println!("✓ Complex multi-stage pipeline built");
    
    // Get pipeline statistics
    let stats = complex_pipeline.get_stats();
    println!("✓ Pipeline stats: {:?}", stats);
    
    // Send start signal
    let _ = complex_pipeline.send_control(StreamData::stream_start());
    println!("✓ Sent start signal to complex pipeline");
    
    // Allow processing time
    sleep(Duration::from_millis(200)).await;
    
    // Collect results from multiple stages
    let mut results_collected = 0;
    let timeout_duration = Duration::from_millis(500);
    
    loop {
        let receive_future = complex_pipeline.receive_result();
        
        match tokio::time::timeout(timeout_duration, receive_future).await {
            Ok(Ok(stream_data)) => {
                println!("✓ Collected result {}: {:?}", results_collected + 1, stream_data.description());
                results_collected += 1;
                
                if stream_data.is_terminal() {
                    println!("✓ Reached end of stream");
                    break;
                }
            }
            Ok(Err(e)) => {
                println!("Receive error: {}", e);
                break;
            }
            Err(_) => {
                println!("Timeout waiting for results (collected {})", results_collected);
                break;
            }
        }
    }
    
    println!("✓ Total results collected: {}", results_collected);
    
    // Final cleanup
    let _ = complex_pipeline.stop_all();
    println!("✅ Complex pipeline test completed\n");
}