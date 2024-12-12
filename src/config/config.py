import os
import sys
from pyspark.sql import SparkSession

# Java environment configuration
if sys.platform.startswith('win'):
    # Java config
    os.environ['JAVA_HOME'] = 'C:\\java\\java-se-8u44-ri'
    
    # Set Java memory parameters
    os.environ['_JAVA_OPTIONS'] = '-Xmx512M'
    
    # Set Hadoop environment
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    hadoop_home = os.path.join(project_root, 'hadoop')
    
    if not os.path.exists(hadoop_home):
        os.makedirs(hadoop_home)
        os.makedirs(os.path.join(hadoop_home, 'bin'))
    
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['PATH'] = f"{os.path.join(hadoop_home, 'bin')};{os.environ['PATH']}"
    
    # Python interpreter config
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def get_spark_session():
    """Create and return SparkSession"""
    try:
        spark = (SparkSession.builder
                .appName("ETL Pipeline")
                .master("local[*]")
                .config("spark.driver.memory", "512m")
                .config("spark.executor.memory", "512m")
                .config("spark.hadoop.fs.permissions.umask-mode", "022")
                .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                .getOrCreate())
        
        print("SparkSession created successfully!")
        return spark
    except Exception as e:
        print(f"Error creating SparkSession: {str(e)}")
        raise