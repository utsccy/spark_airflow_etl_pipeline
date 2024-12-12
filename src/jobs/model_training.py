from typing import Dict, Any
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F
from ..utils.ml_utils import split_train_test, save_model

class TitanicModelTrainingJob:
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config

    def load_data(self) -> Dict[str, Any]:
        """Load preprocessed data"""
        # Read preprocessed data
        df = self.spark.read.parquet(self.config["input_path"])
        
        # Read original data for output
        original_df = self.spark.read.csv(
            self.config["original_data_path"],
            header=True,
            inferSchema=True
        )
        
        train_df, test_df = split_train_test(df, test_size=0.2)
        return {
            "train_data": train_df,
            "test_data": test_df,
            "original_data": original_df
        }

    def train_model(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Train model"""
        train_df = data["train_data"]
        test_df = data["test_data"]
        original_df = data["original_data"]

        # Initialize logistic regression model
        lr = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            maxIter=10
        )

        # Train model
        model = lr.fit(train_df)

        # Make predictions
        predictions = model.transform(test_df)
        
        # Join predictions with original data
        predictions_with_features = predictions.join(
            original_df,
            "PassengerId",
            "left"
        ).select(
            "PassengerId",
            "Survived",
            "Pclass",
            "Name",
            "Sex",
            "Age",
            "SibSp",
            "Parch",
            "Ticket",
            "Fare",
            "Cabin",
            "Embarked",
            "prediction",
            F.col("probability").cast("string").alias("probability")
        )

        # Evaluate model
        evaluator = BinaryClassificationEvaluator(
            rawPredictionCol="rawPrediction",
            labelCol="label"
        )
        auc = evaluator.evaluate(predictions)

        return {
            "model": model,
            "predictions": predictions_with_features,
            "metrics": {"auc": auc}
        }

    def save_results(self, data: Dict[str, Any]) -> None:
        """Save model and predictions"""
        model = data["model"]
        predictions = data["predictions"]
        metrics = data["metrics"]

        # Save model
        save_model(model, self.config["model_path"])

        # Save predictions
        predictions.write.parquet(
            self.config["predictions_path"],
            mode="overwrite"
        )

        # Log metrics
        print(f"Model performance - AUC: {metrics['auc']}")

    def run(self):
        """Run model training pipeline"""
        try:
            data = self.load_data()
            results = self.train_model(data)
            self.save_results(results)
        except Exception as e:
            raise Exception(f"Model training failed: {str(e)}") 