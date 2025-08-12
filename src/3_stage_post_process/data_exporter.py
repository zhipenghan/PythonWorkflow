#!/usr/bin/env python3
"""
Simple Data Exporter Component
===============================

A basic data export component that saves processed data
in various formats for sharing and storage.

This component handles different output formats and file organization.
"""

import argparse
import pandas as pd
import json
from pathlib import Path
import logging
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def export_to_csv(df, output_path):
    """Export data to CSV format."""
    logger.info(f"Exporting to CSV: {output_path}")
    df.to_csv(output_path, index=False)
    return output_path

def export_to_json(df, output_path):
    """Export data to JSON format."""
    logger.info(f"Exporting to JSON: {output_path}")
    df.to_json(output_path, orient='records', indent=2)
    return output_path

def export_to_excel(df, output_path):
    """Export data to Excel format."""
    logger.info(f"Exporting to Excel: {output_path}")
    try:
        df.to_excel(output_path, index=False)
        return output_path
    except ImportError:
        logger.warning("Excel export requires openpyxl. Falling back to CSV.")
        csv_path = output_path.with_suffix('.csv')
        return export_to_csv(df, csv_path)

def create_data_archive(files, archive_path):
    """Create a compressed archive of exported files."""
    logger.info(f"Creating archive: {archive_path}")

    try:
        import zipfile

        with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files:
                if Path(file_path).exists():
                    zipf.write(file_path, Path(file_path).name)
                    logger.info(f"Added {Path(file_path).name} to archive")

        logger.info(f"Archive created successfully: {archive_path}")
        return archive_path

    except ImportError:
        logger.warning("zipfile not available, skipping archive creation")
        return None

def generate_export_summary(df, exported_files):
    """Generate a summary of the export process."""
    logger.info("Generating export summary")

    summary = {
        'export_timestamp': pd.Timestamp.now().isoformat(),
        'dataset_info': {
            'rows': len(df),
            'columns': len(df.columns),
            'column_names': list(df.columns),
            'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024**2, 2)
        },
        'exported_files': []
    }

    for file_path in exported_files:
        if Path(file_path).exists():
            file_info = {
                'filename': Path(file_path).name,
                'format': Path(file_path).suffix.lower().replace('.', ''),
                'size_kb': round(Path(file_path).stat().st_size / 1024, 2),
                'path': str(file_path)
            }
            summary['exported_files'].append(file_info)

    return summary

def main():
    parser = argparse.ArgumentParser(description='Simple Data Export Component')
    parser.add_argument('--input-path', type=str, required=True, help='Path to input data file')
    parser.add_argument('--output-dir', type=str, required=True, help='Directory to save exported files')
    parser.add_argument('--formats', nargs='+', choices=['csv', 'json', 'excel'], 
                       default=['csv'], help='Export formats')
    parser.add_argument('--filename', type=str, default='exported_data', 
                       help='Base filename for exported files')
    parser.add_argument('--create-archive', action='store_true', 
                       help='Create compressed archive of exported files')
    parser.add_argument('--save-summary', action='store_true',
                       help='Save export summary as JSON')

    args = parser.parse_args()

    try:
        # Load data
        logger.info(f"Loading data from {args.input_path}")

        # Try to load as CSV first, then JSON
        try:
            df = pd.read_csv(args.input_path)
        except:
            try:
                with open(args.input_path, 'r') as f:
                    data = json.load(f)
                df = pd.DataFrame(data)
            except:
                logger.error("Could not load data. Supported formats: CSV, JSON")
                raise

        logger.info(f"Loaded data with shape: {df.shape}")

        # Create output directory
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Export to specified formats
        exported_files = []

        for format_type in args.formats:
            if format_type == 'csv':
                output_path = output_dir / f"{args.filename}.csv"
                exported_files.append(export_to_csv(df, output_path))

            elif format_type == 'json':
                output_path = output_dir / f"{args.filename}.json"
                exported_files.append(export_to_json(df, output_path))

            elif format_type == 'excel':
                output_path = output_dir / f"{args.filename}.xlsx"
                exported_files.append(export_to_excel(df, output_path))

        # Generate and save summary
        summary = generate_export_summary(df, exported_files)

        if args.save_summary:
            summary_path = output_dir / f"{args.filename}_summary.json"
            with open(summary_path, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            logger.info(f"Export summary saved to: {summary_path}")
            exported_files.append(summary_path)

        # Create archive if requested
        archive_path = None
        if args.create_archive:
            archive_path = output_dir / f"{args.filename}_archive.zip"
            archive_result = create_data_archive(exported_files, archive_path)
            if archive_result:
                exported_files.append(archive_result)

        # Print summary
        print(f"✅ Data export completed successfully!")
        print(f"📁 Output directory: {output_dir}")
        print(f"📄 Exported {len(exported_files)} files:")

        for file_path in exported_files:
            if Path(file_path).exists():
                size_kb = round(Path(file_path).stat().st_size / 1024, 2)
                print(f"   - {Path(file_path).name} ({size_kb} KB)")

    except Exception as e:
        logger.error(f"Error in data export: {str(e)}")
        print(f"❌ Data export failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
