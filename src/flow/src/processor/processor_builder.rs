//! Processor builder - converts physical plans to stream processors
//! 
//! This module builds a tree of StreamProcessors from a PhysicalPlan tree,
//! following the pattern of rstream's ExecutorBuilder.
//! 
//! Updated for single input, multiple output architecture.

use std::sync::Arc;
use tokio::sync::broadcast;
use crate::planner::physical::PhysicalPlan;
use crate::processor::{StreamProcessor, DataSourceProcessor, FilterProcessor, ProjectProcessor};
use crate::processor::stream_data::StreamData;

/// Result of building a processor - contains the processor and its output channel
pub struct ProcessorBuildResult {
    /// The built processor
    pub processor: Arc<dyn StreamProcessor>,
    /// Channel for receiving data from this processor (StreamData directly, no Result wrapper)
    pub output_receiver: broadcast::Receiver<StreamData>,
}

impl std::fmt::Debug for ProcessorBuildResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessorBuildResult")
            .field("processor", &"<dyn StreamProcessor>")
            .field("output_receiver", &"<Receiver>")
            .finish()
    }
}

/// Builder for creating StreamProcessors from PhysicalPlans
pub struct ProcessorBuilder;

impl ProcessorBuilder {
    /// Build a processor from a physical plan
    pub fn build_processor(&self, physical_plan: Arc<dyn PhysicalPlan>) -> Result<ProcessorBuildResult, String> {
        match physical_plan.get_plan_type() {
            "PhysicalDataSource" => self.build_datasource_processor(physical_plan),
            "PhysicalFilter" => self.build_filter_processor(physical_plan),
            "PhysicalProject" => self.build_project_processor(physical_plan),
            _ => Err(format!("Unknown physical plan type: {}", physical_plan.get_plan_type())),
        }
    }
    
    /// Build a DataSourceProcessor from PhysicalDataSource
    fn build_datasource_processor(&self, physical_plan: Arc<dyn PhysicalPlan>) -> Result<ProcessorBuildResult, String> {
        // For now, assume 1 downstream processor
        let downstream_count = 1;
        
        let processor = Arc::new(DataSourceProcessor::new(
            physical_plan.clone(),
            downstream_count,
        ));
        
        let (input_sender, input_receiver) = processor.create_input_channel();
        let processor_view = processor.start(input_receiver);
        
        // Get output from the first output sender
        let output_receiver = processor_view.output_sender(0)
            .ok_or("DataSourceProcessor has no outputs")?
            .subscribe();
        
        Ok(ProcessorBuildResult {
            processor,
            output_receiver,
        })
    }
    
    /// Build a FilterProcessor from PhysicalFilter
    fn build_filter_processor(&self, physical_plan: Arc<dyn PhysicalPlan>) -> Result<ProcessorBuildResult, String> {
        // For now, assume 1 downstream processor
        let downstream_count = 1;
        
        let processor = Arc::new(FilterProcessor::new(
            physical_plan.clone(),
            downstream_count,
        ));
        
        let (input_sender, input_receiver) = processor.create_input_channel();
        let processor_view = processor.start(input_receiver);
        
        // Get output from the first output sender
        let output_receiver = processor_view.output_sender(0)
            .ok_or("FilterProcessor has no outputs")?
            .subscribe();
        
        Ok(ProcessorBuildResult {
            processor,
            output_receiver,
        })
    }
    
    /// Build a ProjectProcessor from PhysicalProject
    fn build_project_processor(&self, physical_plan: Arc<dyn PhysicalPlan>) -> Result<ProcessorBuildResult, String> {
        // For now, assume 1 downstream processor
        let downstream_count = 1;
        
        let processor = Arc::new(ProjectProcessor::new(
            physical_plan.clone(),
            downstream_count,
        ));
        
        let (input_sender, input_receiver) = processor.create_input_channel();
        let processor_view = processor.start(input_receiver);
        
        // Get output from the first output sender
        let output_receiver = processor_view.output_sender(0)
            .ok_or("ProjectProcessor has no outputs")?
            .subscribe();
        
        Ok(ProcessorBuildResult {
            processor,
            output_receiver,
        })
    }
}