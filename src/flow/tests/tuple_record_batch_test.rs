//! Tests for tuple_to_record_batch with the new HashMap-based Tuple design

use flow::tuple_to_record_batch;
use flow::model::Tuple;
use datatypes::Value;
use std::collections::HashMap;

#[test]
fn test_tuple_to_record_batch_basic() {
    println!("\n=== Testing tuple_to_record_batch Basic Functionality ===");
    
    // Create a tuple with mixed data types
    let mut data = HashMap::new();
    data.insert(("users".to_string(), "id".to_string()), Value::Int64(1));
    data.insert(("users".to_string(), "name".to_string()), Value::String("Alice".to_string()));
    data.insert(("users".to_string(), "age".to_string()), Value::Int64(25));
    data.insert(("users".to_string(), "score".to_string()), Value::Float64(98.5));
    data.insert(("users".to_string(), "active".to_string()), Value::Bool(true));
    
    let tuple = Tuple::new(data);
    
    // Convert to RecordBatch
    let result = tuple_to_record_batch(&tuple);
    if result.is_err() {
        println!("Error: {:?}", result.as_ref().unwrap_err());
    }
    assert!(result.is_ok(), "Conversion should succeed");
    
    let record_batch = result.unwrap();
    println!("Successfully created RecordBatch with {} columns and {} rows", 
             record_batch.num_columns(), record_batch.num_rows());
    
    // Verify the schema and data
    let schema = record_batch.schema();
    println!("Schema fields:");
    for (i, field) in schema.fields().iter().enumerate() {
        println!("  Field {}: {} ({})", i, field.name(), field.data_type());
    }
    
    // Verify we have the expected number of columns
    assert_eq!(record_batch.num_columns(), 5, "Should have 5 columns");
    assert_eq!(record_batch.num_rows(), 1, "Should have 1 row");
    
    println!("✓ Basic tuple to RecordBatch conversion works");
}

#[test]
fn test_tuple_to_record_batch_single_type() {
    println!("\n=== Testing tuple_to_record_batch Single Type ===");
    
    // Create a tuple with only Int64 values
    let mut data = HashMap::new();
    data.insert(("test".to_string(), "col1".to_string()), Value::Int64(10));
    data.insert(("test".to_string(), "col2".to_string()), Value::Int64(20));
    data.insert(("test".to_string(), "col3".to_string()), Value::Int64(30));
    
    let tuple = Tuple::new(data);
    
    // Convert to RecordBatch
    let result = tuple_to_record_batch(&tuple);
    assert!(result.is_ok(), "Conversion should succeed");
    
    let record_batch = result.unwrap();
    println!("Successfully created RecordBatch with {} Int64 columns", record_batch.num_columns());
    
    // Verify all columns are Int64
    let schema = record_batch.schema();
    for field in schema.fields() {
        assert_eq!(field.data_type(), &arrow::datatypes::DataType::Int64);
    }
    
    println!("✓ Single type tuple to RecordBatch conversion works");
}

#[test]
fn test_tuple_to_record_batch_mixed_sources() {
    println!("\n=== Testing tuple_to_record_batch Mixed Sources ===");
    
    // Create a tuple with data from different sources
    let mut data = HashMap::new();
    data.insert(("users".to_string(), "id".to_string()), Value::Int64(1));
    data.insert(("orders".to_string(), "total".to_string()), Value::Float64(99.99));
    data.insert(("products".to_string(), "name".to_string()), Value::String("Widget".to_string()));
    data.insert(("inventory".to_string(), "in_stock".to_string()), Value::Bool(true));
    
    let tuple = Tuple::new(data);
    
    // Convert to RecordBatch
    let result = tuple_to_record_batch(&tuple);
    assert!(result.is_ok(), "Conversion should succeed");
    
    let record_batch = result.unwrap();
    println!("Successfully created RecordBatch with data from multiple sources");
    
    // Verify field names include source.column format
    let schema = record_batch.schema();
    let expected_fields = vec![
        "users.id",
        "orders.total", 
        "products.name",
        "inventory.in_stock"
    ];
    
    for field_name in expected_fields {
        let found = schema.fields().iter().any(|f| f.name() == field_name);
        assert!(found, "Should find field: {}", field_name);
    }
    
    println!("✓ Mixed sources tuple to RecordBatch conversion works");
}

#[test]
fn test_tuple_to_record_batch_empty() {
    println!("\n=== Testing tuple_to_record_batch Empty Tuple ===");
    
    // Create an empty tuple
    let data = HashMap::new();
    let tuple = Tuple::new(data);
    
    // Convert to RecordBatch - should fail
    let result = tuple_to_record_batch(&tuple);
    assert!(result.is_err(), "Conversion should fail for empty tuple");
    
    let error = result.unwrap_err();
    println!("Correctly failed with error: {}", error);
    assert!(error.to_string().contains("empty"));
    
    println!("✓ Empty tuple correctly fails conversion");
}

#[test]
fn test_tuple_to_record_batch_null_values() {
    println!("\n=== Testing tuple_to_record_batch with Null Values ===");
    
    // Create a tuple with some null values
    let mut data = HashMap::new();
    data.insert(("test".to_string(), "value1".to_string()), Value::Int64(42));
    data.insert(("test".to_string(), "value2".to_string()), Value::Null);
    data.insert(("test".to_string(), "value3".to_string()), Value::String("hello".to_string()));
    
    let tuple = Tuple::new(data);
    
    // Convert to RecordBatch - null values should be skipped
    let result = tuple_to_record_batch(&tuple);
    assert!(result.is_ok(), "Conversion should succeed");
    
    let record_batch = result.unwrap();
    println!("Successfully created RecordBatch, null values were skipped");
    
    // Should have 2 columns (null value skipped)
    assert_eq!(record_batch.num_columns(), 2, "Should have 2 columns (null skipped)");
    
    println!("✓ Tuple with null values conversion works");
}

#[test]
fn test_tuple_to_record_batch_type_inference() {
    println!("\n=== Testing tuple_to_record_batch Type Inference ===");
    
    // Test different numeric types are correctly inferred
    let mut data = HashMap::new();
    data.insert(("test".to_string(), "int8".to_string()), Value::Int8(1));
    data.insert(("test".to_string(), "int16".to_string()), Value::Int16(100));
    data.insert(("test".to_string(), "int32".to_string()), Value::Int32(1000));
    data.insert(("test".to_string(), "int64".to_string()), Value::Int64(10000));
    data.insert(("test".to_string(), "float32".to_string()), Value::Float32(3.15));
    data.insert(("test".to_string(), "float64".to_string()), Value::Float64(2.79));
    data.insert(("test".to_string(), "uint8".to_string()), Value::Uint8(255));
    data.insert(("test".to_string(), "uint16".to_string()), Value::Uint16(65535));
    data.insert(("test".to_string(), "uint32".to_string()), Value::Uint32(4294967295));
    data.insert(("test".to_string(), "uint64".to_string()), Value::Uint64(18446744073709551615));
    
    let tuple = Tuple::new(data);
    
    // Convert to RecordBatch
    let result = tuple_to_record_batch(&tuple);
    assert!(result.is_ok(), "Conversion should succeed");
    
    let record_batch = result.unwrap();
    println!("Successfully created RecordBatch with various numeric types");
    
    // Verify type consistency - types should be preserved as-is
    let schema = record_batch.schema();
    
    // Each type should remain its original type for consistency
    let int8_fields: Vec<_> = schema.fields().iter()
        .filter(|f| f.data_type() == &arrow::datatypes::DataType::Int8)
        .collect();
    assert!(int8_fields.len() >= 1, "Should have at least 1 Int8 field");
    
    let int16_fields: Vec<_> = schema.fields().iter()
        .filter(|f| f.data_type() == &arrow::datatypes::DataType::Int16)
        .collect();
    assert!(int16_fields.len() >= 1, "Should have at least 1 Int16 field");
    
    let int32_fields: Vec<_> = schema.fields().iter()
        .filter(|f| f.data_type() == &arrow::datatypes::DataType::Int32)
        .collect();
    assert!(int32_fields.len() >= 1, "Should have at least 1 Int32 field");
    
    let int64_fields: Vec<_> = schema.fields().iter()
        .filter(|f| f.data_type() == &arrow::datatypes::DataType::Int64)
        .collect();
    assert!(int64_fields.len() >= 1, "Should have at least 1 Int64 field");
    
    let float32_fields: Vec<_> = schema.fields().iter()
        .filter(|f| f.data_type() == &arrow::datatypes::DataType::Float32)
        .collect();
    assert!(float32_fields.len() >= 1, "Should have at least 1 Float32 field");
    
    let float64_fields: Vec<_> = schema.fields().iter()
        .filter(|f| f.data_type() == &arrow::datatypes::DataType::Float64)
        .collect();
    assert!(float64_fields.len() >= 1, "Should have at least 1 Float64 field");
    
    let uint8_fields: Vec<_> = schema.fields().iter()
        .filter(|f| f.data_type() == &arrow::datatypes::DataType::UInt8)
        .collect();
    assert!(uint8_fields.len() >= 1, "Should have at least 1 UInt8 field");
    
    let uint16_fields: Vec<_> = schema.fields().iter()
        .filter(|f| f.data_type() == &arrow::datatypes::DataType::UInt16)
        .collect();
    assert!(uint16_fields.len() >= 1, "Should have at least 1 UInt16 field");
    
    let uint32_fields: Vec<_> = schema.fields().iter()
        .filter(|f| f.data_type() == &arrow::datatypes::DataType::UInt32)
        .collect();
    assert!(uint32_fields.len() >= 1, "Should have at least 1 UInt32 field");
    
    let uint64_fields: Vec<_> = schema.fields().iter()
        .filter(|f| f.data_type() == &arrow::datatypes::DataType::UInt64)
        .collect();
    assert!(uint64_fields.len() >= 1, "Should have at least 1 UInt64 field");
    
    println!("✓ Type consistency maintained for various numeric types");
}