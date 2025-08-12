# PythonWorkflow Framework

A simple, modular framework for building data processing pipelines in Python. This framework demonstrates how to structure and orchestrate data processing workflows with reusable components.

## 🎯 Purpose

This framework is designed to showcase:
- **Pipeline Orchestration**: How to organize and execute data processing steps
- **Modular Components**: Reusable data processing components
- **Configuration-Driven Workflows**: YAML-based pipeline definitions
- **Simple Architecture**: Easy-to-understand 3-stage processing pattern

## 🏗️ Architecture

```
┌─────────────────┬─────────────────┬─────────────────┐
│   Pre-Process   │     Process     │  Post-Process   │
├─────────────────┼─────────────────┼─────────────────┤
│ • Data Loading  │ • Transformation│ • Report Gen    │
│ • Data Cleaning │ • Analysis      │ • Data Export   │
│ • Validation    │ • Aggregation   │ • Archiving     │
└─────────────────┴─────────────────┴─────────────────┘
```

### Stage 1: Pre-Processing
- **Data Loader**: Load data from various sources (CSV, JSON, Excel)
- **Data Validator**: Validate data quality and schema

### Stage 2: Processing  
- **Data Transformer**: Apply calculations, aggregations, and formatting
- **Data Analyzer**: Generate statistics and insights

### Stage 3: Post-Processing
- **Report Generator**: Create HTML/Markdown reports
- **Data Exporter**: Export results in multiple formats

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run Sample Pipeline
```bash
python run_pipeline.py pipelines/simple_pipeline.yaml
```

### 3. Check Results
The pipeline will create:
- `data/processed/` - Intermediate processing files
- `data/results/` - Analysis reports  
- `data/final/` - Final exported data and archive

## 📁 Project Structure

```
PythonWorkflow/
├── run_pipeline.py              # Pipeline runner
├── requirements.txt             # Dependencies
├── README.md                   # This file
├── pipelines/                  # Pipeline configurations
│   ├── simple_pipeline.yaml   # Basic processing pipeline
│   └── pipeline_sample_e2e.yaml # Legacy complex pipeline
└── src/                       # Processing components
    ├── 1_stage_pre_process/   # Data loading & validation
    │   ├── data_loader.py     # Load and clean data
    │   └── data_validator.py  # Validate data quality
    ├── 2_stage_process/       # Data transformation
    │   ├── feature_engineering.py # Data transformations
    │   └── data_analyzer.py   # Statistical analysis
    └── 3_stage_post_process/  # Results & export
        ├── simple_reporter.py # Generate reports
        └── data_exporter.py   # Export to formats
```

## 🔧 Usage Examples

### Generate Sample Data and Process
```bash
# Load sample data
python src/1_stage_pre_process/data_loader.py \
  --generate-sample \
  --output-path data/sample.csv

# Transform the data  
python src/2_stage_process/feature_engineering.py \
  --input-path data/sample.csv \
  --output-path data/transformed.csv \
  --add-calculations \
  --format-data

# Generate a report
python src/3_stage_post_process/simple_reporter.py \
  --input-path data/transformed.csv \
  --output-path data/report.html \
  --format html
```

### Create Custom Pipeline
Create a YAML file with your workflow:

```yaml
name: "My Custom Pipeline"
description: "Custom data processing workflow"

steps:
  - id: "load"
    component: "src/1_stage_pre_process/data_loader.py"
    parameters:
      --input-path: "my_data.csv"
      --output-path: "data/loaded.csv"
      --clean-data: true
  
  - id: "process"
    component: "src/2_stage_process/feature_engineering.py"
    depends_on: ["load"]
    parameters:
      --input-path: "data/loaded.csv" 
      --output-path: "data/processed.csv"
      --add-calculations: true
```

## �️ Components

### Data Loader (`data_loader.py`)
- Load CSV, JSON, Excel files
- Generate sample datasets
- Basic data cleaning
- Remove duplicates and empty rows

### Data Transformer (`feature_engineering.py`)  
- Add calculated fields (totals, dates)
- Create aggregations by category
- Apply data formatting
- Filter data by criteria

### Data Analyzer (`data_analyzer.py`)
- Generate descriptive statistics
- Find data quality issues
- Identify patterns and outliers
- Create insights and recommendations

### Report Generator (`simple_reporter.py`)
- Create HTML reports with styling
- Generate Markdown summaries
- Export analysis as JSON
- Include data statistics and insights

### Data Exporter (`data_exporter.py`)
- Export to CSV, JSON, Excel formats
- Create compressed archives
- Generate export summaries
- Organize output files

## � Key Features

- **Modular Design**: Each component is independent and reusable
- **Pipeline Configuration**: Define workflows in YAML files
- **Multiple Formats**: Support for CSV, JSON, Excel, HTML, Markdown
- **Error Handling**: Comprehensive logging and error reporting
- **Dependency Management**: Automatic step ordering and dependencies
- **Flexible Parameters**: Command-line configuration for all components

## 📊 Example Output

After running the simple pipeline:

```
✅ Pipeline completed successfully!
📊 Results Summary:
  - Generated sample data: 100 rows, 6 columns
  - Created 8 derived fields
  - Found 3 data insights  
  - Exported 2 formats: CSV, JSON
  - Generated HTML report: data/results/report.html
```
```

## 🤝 Contributing

This framework is designed as a demonstration of pipeline architecture patterns. Feel free to:
- Add new processing components
- Create custom pipeline configurations  
- Extend the component interfaces
- Improve error handling and logging

## � License

MIT License - see LICENSE file for details.

---

**PythonWorkflow Framework** - Simple, modular data processing pipelines

- **Data Analysis Pipelines**: ETL processes for research data
- **Machine Learning Workflows**: From data prep to model deployment
- **Report Generation**: Automated analysis and reporting
- **Batch Processing**: Large-scale data processing jobs
- **Experimental Workflows**: Reproducible research experiments

## 🔗 Related Tools

This framework is designed to be lightweight and can be integrated with:
- **Apache Airflow** for production scheduling
- **MLflow** for experiment tracking
- **Docker** for containerized execution
- **Jupyter Notebooks** for interactive development
