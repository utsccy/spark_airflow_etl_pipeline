import os
from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.ml import Pipeline, PipelineModel

def ensure_dir(path: str):
    """Ensure directory exists"""
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

def split_train_test(
    df: DataFrame,
    test_size: float = 0.2,
    seed: int = 42
) -> Tuple[DataFrame, DataFrame]:
    """Split data into training and test sets"""
    train_df, test_df = df.randomSplit([1 - test_size, test_size], seed=seed)
    return train_df, test_df

def save_model(model: PipelineModel, path: str) -> None:
    """Save model with overwrite option"""
    try:
        ensure_dir(path)
        model.write().overwrite().save(path)
    except Exception as e:
        raise Exception(f"Failed to save model: {str(e)}")

def load_model(path: str) -> PipelineModel:
    """Load saved model"""
    try:
        return PipelineModel.load(path)
    except Exception as e:
        raise Exception(f"Failed to load model: {str(e)}") 