#!/usr/bin/env python3
"""
Simple Data Transformer Component
==================================

A basic data transformation component that demonstrates how to process
data in the middle stage of a pipeline workflow.

This component performs simple transformations like calculations,
formatting, and aggregations.
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def add_calculated_fields(df):
    """Add simple calculated fields based on existing data."""
    logger.info("Adding calculated fields")
    
    if 'value' in df.columns and 'quantity' in df.columns:
        df['total_value'] = df['value'] * df['quantity']
        logger.info("Added 'total_value' field")
    
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['day_of_week'] = df['date'].dt.dayofweek
        logger.info("Added date-based fields")
    
    return df

def create_aggregations(df, group_by_col='category'):
    """Create summary aggregations."""
    logger.info(f"Creating aggregations grouped by '{group_by_col}'")
    
    if group_by_col not in df.columns:
        logger.warning(f"Column '{group_by_col}' not found, skipping aggregations")
        return df
    
    # Create aggregation summary
    agg_stats = df.groupby(group_by_col).agg({
        'value': ['count', 'mean', 'sum'],
        'quantity': ['mean', 'sum'] if 'quantity' in df.columns else 'count'
    }).round(2)
    
    # Flatten column names
    agg_stats.columns = ['_'.join(col).strip() for col in agg_stats.columns]
    
    # Merge back to original data
    df = df.merge(agg_stats, left_on=group_by_col, right_index=True, how='left')
    
    logger.info(f"Added {len(agg_stats.columns)} aggregation fields")
    return df

def format_data(df):
    """Apply formatting to the data."""
    logger.info("Applying data formatting")
    
    # Round numeric columns to 2 decimal places
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        df[col] = df[col].round(2)
    
    # Convert category to title case
    if 'category' in df.columns:
        df['category'] = df['category'].str.title()
    
    logger.info("Applied formatting to numeric and text fields")
    return df

def apply_filters(df, min_value=None, max_value=None, categories=None):
    """Apply filtering criteria."""
    logger.info("Applying filters")
    initial_rows = len(df)
    
    if min_value is not None and 'value' in df.columns:
        df = df[df['value'] >= min_value]
        logger.info(f"Applied min_value filter: {min_value}")
    
    if max_value is not None and 'value' in df.columns:
        df = df[df['value'] <= max_value]
        logger.info(f"Applied max_value filter: {max_value}")
    
    if categories and 'category' in df.columns:
        df = df[df['category'].isin(categories)]
        logger.info(f"Applied category filter: {categories}")
    
    logger.info(f"Filtered from {initial_rows} to {len(df)} rows")
    return df

def main():
    parser = argparse.ArgumentParser(description='Simple Data Transformation Component')
    parser.add_argument('--input-path', type=str, required=True, help='Path to input data file')
    parser.add_argument('--output-path', type=str, required=True, help='Path to save transformed data')
    parser.add_argument('--add-calculations', action='store_true', help='Add calculated fields')
    parser.add_argument('--create-aggregations', action='store_true', help='Create aggregation fields')
    parser.add_argument('--group-by', type=str, default='category', help='Column to group by for aggregations')
    parser.add_argument('--format-data', action='store_true', help='Apply data formatting')
    parser.add_argument('--min-value', type=float, help='Minimum value filter')
    parser.add_argument('--max-value', type=float, help='Maximum value filter')
    parser.add_argument('--categories', nargs='+', help='Categories to include in filter')
    
    args = parser.parse_args()
    
    try:
        # Load data
        logger.info(f"Loading data from {args.input_path}")
        df = pd.read_csv(args.input_path)
        logger.info(f"Loaded data with shape: {df.shape}")
        
        # Apply transformations
        if args.add_calculations:
            df = add_calculated_fields(df)
        
        if args.create_aggregations:
            df = create_aggregations(df, args.group_by)
        
        if args.format_data:
            df = format_data(df)
        
        # Apply filters
        if args.min_value or args.max_value or args.categories:
            df = apply_filters(df, args.min_value, args.max_value, args.categories)
        
        # Save transformed data
        output_path = Path(args.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        
        logger.info("Data Transformation Summary:")
        logger.info(f"  - Final shape: {df.shape}")
        logger.info(f"  - Columns: {list(df.columns)}")
        
        print("✅ Data transformation completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in data transformation: {str(e)}")
        print(f"❌ Data transformation failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
