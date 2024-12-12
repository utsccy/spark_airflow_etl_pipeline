# Data Pipeline ETL Solution Using PySpark and Airflow
Create a comprehensive data integration workflow that seamlessly moves and transforms data from multiple sources to a centralized storage system. The goal is to create a scalable, efficient data pipeline that transforms raw, potentially messy data into a clean, structured format ready for business intelligence and decision-making processes.

This project involves:
Data Extraction: Collect raw data from diverse sources including:

-Comma-separated value (CSV) files

-JavaScript Object Notation (JSON) files

-Relational database systems

## Project Structure

- `src/`: Source code directory
  - `config/`: Configuration files
  - `jobs/`: ETL jobs
  - `utils/`: Utility functions
- `tests/`: Test files
- `requirements.txt`: Project dependencies

The pipeline will:
- Preprocess the data (handle missing values, encode categorical features)
- Create feature vectors
- Split data into training and test sets
- Train a logistic regression model
- Save the model and predictions

## Project Components

### Data Preprocessing (`src/jobs/data_preprocessing.py`)
- Handles missing values
- Extracts title from passenger names
- Encodes categorical features
- Creates feature vectors

### Model Training (`src/jobs/model_training.py`)
- Implements logistic regression model
- Performs model training and evaluation
- Saves model and predictions

### Configuration (`src/config/config.py`)
- Configures Spark session
- Sets up environment variables
- Manages memory settings

### Utilities
- `spark_utils.py`: Common Spark operations
- `ml_utils.py`: Machine learning utilities

## Output

The pipeline generates:
- Processed data: `data/processed/titanic_processed/`
- Model files: `models/saved_models/titanic_model/`
- Predictions: `data/predictions/titanic_predictions/`

## Model Performance

The model's performance is evaluated using:
- AUC (Area Under the ROC Curve)
- The metrics are printed during execution

## Airflow Integration

### Setup

1. Install Airflow and dependencies:
```bash
pip install -