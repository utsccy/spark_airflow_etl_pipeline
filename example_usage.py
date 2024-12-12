from src.config.config import get_spark_session
from src.jobs.data_preprocessing import TitanicPreprocessingJob
from src.jobs.model_training import TitanicModelTrainingJob
from src.utils.spark_utils import export_to_csv

def main():
    # Create Spark session
    spark = get_spark_session()

    # Preprocessing configuration
    preprocessing_config = {
        "input_path": "data/Titanic-Dataset.csv",
        "output_path": "data/processed/titanic_processed",
        "readable_output_path": "data/processed/titanic_processed_readable"
    }

    # Model training configuration
    training_config = {
        "input_path": "data/processed/titanic_processed",
        "original_data_path": "data/Titanic-Dataset.csv",
        "model_path": "models/saved_models/titanic_model",
        "predictions_path": "data/predictions/titanic_predictions"
    }

    # Run preprocessing
    preprocessing_job = TitanicPreprocessingJob(spark, preprocessing_config)
    preprocessing_job.run()

    # Run model training
    training_job = TitanicModelTrainingJob(spark, training_config)
    training_job.run()

    # Export results to CSV
    export_to_csv(
        spark, 
        preprocessing_config["readable_output_path"],
        "data/processed/titanic_processed.csv"
    )
    export_to_csv(
        spark, 
        training_config["predictions_path"], 
        "data/predictions/titanic_predictions.csv"
    )

if __name__ == "__main__":
    main() 