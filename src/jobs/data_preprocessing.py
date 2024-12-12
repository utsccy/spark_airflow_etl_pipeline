from typing import Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT
from ..utils.spark_utils import validate_dataframe, write_dataframe

class TitanicPreprocessingJob:
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        
    def extract(self) -> Dict[str, Any]:
        """Read Titanic dataset"""
        try:
            df = self.spark.read.csv(
                self.config["input_path"],
                header=True,
                inferSchema=True
            )
            return {"raw_data": df}
        except Exception as e:
            raise Exception(f"Data extraction failed: {str(e)}")

    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and preprocess data"""
        df = data["raw_data"]
        
        # 1. Handle missing values
        df = df.na.fill({
            "Age": df.select(F.mean("Age")).collect()[0][0],
            "Fare": df.select(F.mean("Fare")).collect()[0][0],
            "Embarked": "S"
        })
        
        # 2. Feature engineering
        # Extract title from name using raw string
        df = df.withColumn("Title", F.regexp_extract(F.col("Name"), r"([A-Za-z]+)\.", 1))
        
        # 3. Encode categorical features
        categorical_columns = ["Sex", "Embarked", "Title"]
        for column in categorical_columns:
            indexer = StringIndexer(
                inputCol=column,
                outputCol=f"{column}_Index",
                handleInvalid="keep"
            )
            df = indexer.fit(df).transform(df)
        
        # 4. Select features
        feature_columns = [
            "Pclass",
            "Sex_Index",
            "Age",
            "SibSp",
            "Parch",
            "Fare",
            "Embarked_Index",
            "Title_Index"
        ]
        
        # 5. Create feature vector for model training
        assembler = VectorAssembler(
            inputCols=feature_columns,
            outputCol="features"
        )
        model_df = assembler.transform(df)
        
        # 6. Prepare label column
        model_df = model_df.withColumn("label", F.col("Survived").cast(DoubleType()))
        
        # 7. Prepare readable output with individual features
        output_df = df.select(
            "PassengerId",
            "Survived",
            "Pclass",
            "Sex_Index",
            "Age",
            "SibSp",
            "Parch",
            "Fare",
            "Embarked_Index",
            "Title_Index"
        )
        
        return {
            "transformed_data": model_df.select("PassengerId", "features", "label"),  # For model training
            "readable_data": output_df  # For CSV export
        }

    def load(self, data: Dict[str, Any]) -> None:
        """Save preprocessed data"""
        # Save vectorized data for model training
        model_df = data["transformed_data"]
        write_dataframe(
            df=model_df,
            output_path=self.config["output_path"],
            format="parquet",
            mode="overwrite"
        )
        
        # Save readable data for analysis
        readable_df = data["readable_data"]
        write_dataframe(
            df=readable_df,
            output_path=self.config["readable_output_path"],
            format="parquet",
            mode="overwrite"
        )

    def run(self):
        """Run preprocessing pipeline"""
        try:
            extracted_data = self.extract()
            transformed_data = self.transform(extracted_data)
            self.load(transformed_data)
        except Exception as e:
            raise Exception(f"Preprocessing job failed: {str(e)}") 