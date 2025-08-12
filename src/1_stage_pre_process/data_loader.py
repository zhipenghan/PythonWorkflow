#!/usr/bin/env python3
"""
Simple Data Loader Component
============================

A basic data loading component that demonstrates how to structure
pipeline components with configurable parameters.

This component loads data from various sources and performs basic operations.
"""

import argparse
import pandas as pd
import numpy as np
import json
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data(input_path, file_format='csv'):
    """Load data from specified path and format."""
    input_path = Path(input_path)
    
    if file_format.lower() == 'csv':
        logger.info(f"Loading CSV file: {input_path}")
        return pd.read_csv(input_path)
    elif file_format.lower() == 'json':
        logger.info(f"Loading JSON file: {input_path}")
        with open(input_path, 'r') as f:
            data = json.load(f)
        return pd.DataFrame(data)
    elif file_format.lower() == 'excel':
        logger.info(f"Loading Excel file: {input_path}")
        return pd.read_excel(input_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

def clean_data(df, remove_duplicates=True):
    """Basic data cleaning."""
    logger.info("Starting basic data cleaning")
    
    initial_rows = len(df)
    logger.info(f"Initial dataset size: {initial_rows} rows, {len(df.columns)} columns")
    
    # Remove duplicates
    if remove_duplicates:
        df = df.drop_duplicates()
        logger.info(f"Removed {initial_rows - len(df)} duplicate rows")
    
    # Remove empty rows
    df = df.dropna(how='all')
    logger.info(f"Final size: {len(df)} rows")
    
    return df

def generate_sample_data(output_path):
    """Generate sample data for demonstration purposes."""
    logger.info("Generating sample dataset")
    
    np.random.seed(42)
    n_samples = 100
    
    data = {
        'id': range(1, n_samples + 1),
        'name': [f'Item_{i}' for i in range(1, n_samples + 1)],
        'category': np.random.choice(['A', 'B', 'C'], n_samples),
        'value': np.random.uniform(10, 100, n_samples),
        'quantity': np.random.randint(1, 10, n_samples),
        'date': pd.date_range('2024-01-01', periods=n_samples, freq='D')
    }
    
    df = pd.DataFrame(data)
    
    # Save data
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    
    logger.info(f"Generated sample data with {len(df)} rows: {output_path}")
    return df

def main():
    parser = argparse.ArgumentParser(description='Simple Data Loading Component')
    parser.add_argument('--input-path', type=str, help='Path to input data')
    parser.add_argument('--output-path', type=str, required=True, help='Path to save processed data')
    parser.add_argument('--file-format', type=str, default='csv', choices=['csv', 'json', 'excel'], 
                       help='Input/output file format')
    parser.add_argument('--clean-data', action='store_true', help='Enable data cleaning')
    parser.add_argument('--generate-sample', action='store_true', 
                       help='Generate sample data instead of loading from input')
    
    args = parser.parse_args()
    
    try:
        if args.generate_sample:
            df = generate_sample_data(args.output_path)
        else:
            if not args.input_path:
                raise ValueError("--input-path is required when not generating sample data")
            
            # Load data
            df = load_data(args.input_path, args.file_format)
            
            # Clean data if requested
            if args.clean_data:
                df = clean_data(df)
            
            # Save processed data
            output_path = Path(args.output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)
            logger.info(f"Saved processed data to: {output_path}")
        
        # Print summary
        logger.info("Data Loading Summary:")
        logger.info(f"  - Dataset shape: {df.shape}")
        logger.info(f"  - Columns: {list(df.columns)}")
        
        print("✅ Data loading completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in data loading: {str(e)}")
        print(f"❌ Data loading failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
