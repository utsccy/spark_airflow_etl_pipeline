from typing import List, Dict, Any
from pyspark.sql import DataFrame
import logging
from pyspark.sql import functions as F
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ensure_dir(path: str):
    """Ensure directory exists"""
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

def validate_dataframe(df: DataFrame, required_columns: List[str]) -> bool:
    """Validate if DataFrame contains required columns"""
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    return True

def write_dataframe(df: DataFrame, output_path: str, format: str = "parquet", 
                   mode: str = "overwrite", **options: Dict[str, Any]):
    """Write DataFrame to specified location"""
    try:
        ensure_dir(output_path)
        
        # Configure Spark to not try to modify permissions
        writer = df.write.format(format).mode(mode) \
            .option("fs.permissions.umask-mode", "022") \
            .option("hadoop.job.ugi", "airflow")
        
        # Add additional write options
        for key, value in options.items():
            writer = writer.option(key, value)
            
        writer.save(output_path)
        logger.info(f"Data successfully written to: {output_path}")
    except Exception as e:
        logger.error(f"Error writing data: {str(e)}")
        raise

def export_to_csv(spark, parquet_path: str, csv_path: str):
    """Export Parquet file to CSV format"""
    try:
        ensure_dir(csv_path)
        
        # Read parquet file
        df = spark.read.parquet(parquet_path)
        
        # Convert complex types to strings
        for column in df.columns:
            col_type = str(df.schema[column].dataType)
            if "struct" in col_type or "array" in col_type or "vector" in col_type.lower():
                df = df.withColumn(column, F.col(column).cast("string"))
        
        # Write as single CSV file with specific permissions
        df.coalesce(1).write \
            .option("fs.permissions.umask-mode", "022") \
            .option("hadoop.job.ugi", "airflow") \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(csv_path)
        
        logger.info(f"Data exported to CSV: {csv_path}")
    except Exception as e:
        logger.error(f"Error exporting to CSV: {str(e)}")
        raise