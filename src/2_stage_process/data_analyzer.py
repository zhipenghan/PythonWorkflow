#!/usr/bin/env python3
"""
Simple Data Analyzer Component
===============================

A basic data analysis component that demonstrates how to analyze
and summarize data in a pipeline workflow.

This component performs statistical analysis and generates insights
without complex machine learning.
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

def analyze_data_quality(df):
    """Analyze data quality and completeness."""
    logger.info("Analyzing data quality")
    
    quality_report = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'missing_values': df.isnull().sum().to_dict(),
        'duplicate_rows': df.duplicated().sum(),
        'data_types': df.dtypes.astype(str).to_dict()
    }
    
    logger.info(f"Data quality analysis complete: {len(df)} rows, {len(df.columns)} columns")
    return quality_report

def generate_statistics(df):
    """Generate descriptive statistics."""
    logger.info("Generating descriptive statistics")
    
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    categorical_cols = df.select_dtypes(include=['object']).columns
    
    stats = {
        'numeric_summary': df[numeric_cols].describe().to_dict() if len(numeric_cols) > 0 else {},
        'categorical_summary': {},
        'correlations': {}
    }
    
    # Categorical analysis
    for col in categorical_cols:
        stats['categorical_summary'][col] = {
            'unique_values': df[col].nunique(),
            'most_common': df[col].mode().iloc[0] if len(df[col].mode()) > 0 else None,
            'value_counts': df[col].value_counts().head().to_dict()
        }
    
    # Correlation analysis for numeric columns
    if len(numeric_cols) > 1:
        correlations = df[numeric_cols].corr()
        stats['correlations'] = correlations.to_dict()
    
    logger.info(f"Statistics generated for {len(numeric_cols)} numeric and {len(categorical_cols)} categorical columns")
    return stats

def find_insights(df):
    """Find interesting patterns and insights in the data."""
    logger.info("Finding data insights")
    
    insights = []
    
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    # Find outliers using IQR method
    for col in numeric_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        outliers = df[(df[col] < Q1 - 1.5 * IQR) | (df[col] > Q3 + 1.5 * IQR)]
        
        if len(outliers) > 0:
            insights.append({
                'type': 'outliers',
                'column': col,
                'count': len(outliers),
                'percentage': (len(outliers) / len(df)) * 100
            })
    
    # Find columns with high missing values
    missing_pct = (df.isnull().sum() / len(df)) * 100
    high_missing = missing_pct[missing_pct > 20]
    
    for col in high_missing.index:
        insights.append({
            'type': 'high_missing',
            'column': col,
            'missing_percentage': high_missing[col]
        })
    
    # Find highly correlated pairs
    if len(numeric_cols) > 1:
        corr_matrix = df[numeric_cols].corr()
        
        # Find pairs with correlation > 0.8 or < -0.8
        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                corr_val = corr_matrix.iloc[i, j]
                if abs(corr_val) > 0.8:
                    insights.append({
                        'type': 'high_correlation',
                        'column_1': corr_matrix.columns[i],
                        'column_2': corr_matrix.columns[j],
                        'correlation': corr_val
                    })
    
    logger.info(f"Found {len(insights)} insights")
    return insights

def create_summary_report(df, quality_report, stats, insights):
    """Create a comprehensive summary report."""
    logger.info("Creating summary report")
    
    report = {
        'dataset_overview': {
            'name': 'Analysis Report',
            'timestamp': pd.Timestamp.now().isoformat(),
            'shape': df.shape,
            'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024**2, 2)
        },
        'data_quality': quality_report,
        'statistics': stats,
        'insights': insights,
        'recommendations': []
    }
    
    # Add recommendations based on findings
    if quality_report['duplicate_rows'] > 0:
        report['recommendations'].append("Consider removing duplicate rows")
    
    for insight in insights:
        if insight['type'] == 'high_missing':
            report['recommendations'].append(f"Investigate missing values in {insight['column']}")
        elif insight['type'] == 'outliers':
            report['recommendations'].append(f"Review outliers in {insight['column']}")
    
    logger.info("Summary report created successfully")
    return report

def main():
    parser = argparse.ArgumentParser(description='Simple Data Analysis Component')
    parser.add_argument('--input-path', type=str, required=True, help='Path to input data file')
    parser.add_argument('--output-path', type=str, required=True, help='Path to save analysis report')
    parser.add_argument('--save-format', type=str, choices=['json', 'csv'], default='json', 
                       help='Format to save the analysis report')
    
    args = parser.parse_args()
    
    try:
        # Load data
        logger.info(f"Loading data from {args.input_path}")
        df = pd.read_csv(args.input_path)
        logger.info(f"Loaded data with shape: {df.shape}")
        
        # Perform analysis
        quality_report = analyze_data_quality(df)
        statistics = generate_statistics(df)
        insights = find_insights(df)
        
        # Create comprehensive report
        report = create_summary_report(df, quality_report, statistics, insights)
        
        # Save analysis report
        output_path = Path(args.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if args.save_format == 'json':
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Analysis report saved as JSON to: {output_path}")
        else:
            # Save as CSV (simplified format)
            summary_df = pd.DataFrame([{
                'metric': 'total_rows',
                'value': report['dataset_overview']['shape'][0]
            }, {
                'metric': 'total_columns', 
                'value': report['dataset_overview']['shape'][1]
            }, {
                'metric': 'insights_found',
                'value': len(report['insights'])
            }, {
                'metric': 'recommendations',
                'value': len(report['recommendations'])
            }])
            summary_df.to_csv(output_path, index=False)
            logger.info(f"Analysis summary saved as CSV to: {output_path}")
        
        # Print key findings
        print(f"✅ Data analysis completed successfully!")
        print(f"📊 Dataset: {df.shape[0]} rows, {df.shape[1]} columns")
        print(f"🔍 Found {len(insights)} insights")
        print(f"💡 Generated {len(report['recommendations'])} recommendations")
        
    except Exception as e:
        logger.error(f"Error in data analysis: {str(e)}")
        print(f"❌ Data analysis failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
