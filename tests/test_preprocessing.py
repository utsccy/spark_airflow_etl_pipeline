import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from src.jobs.data_preprocessing import TitanicPreprocessingJob

@pytest.fixture
def input_schema():
    """Define input data schema"""
    return StructType([
        StructField("PassengerId", IntegerType(), True),
        StructField("Survived", IntegerType(), True),
        StructField("Pclass", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("Sex", StringType(), True),
        StructField("Age", DoubleType(), True),
        StructField("SibSp", IntegerType(), True),
        StructField("Parch", IntegerType(), True),
        StructField("Ticket", StringType(), True),
        StructField("Fare", DoubleType(), True),
        StructField("Cabin", StringType(), True),
        StructField("Embarked", StringType(), True)
    ])

@pytest.fixture
def input_data():
    """Sample input data"""
    return [
        (1, 0, 3, "Braund, Mr. Owen Harris", "male", 22.0, 1, 0, "A/5 21171", 7.25, None, "S"),
        (2, 1, 1, "Cumings, Mrs. John Bradley", "female", 38.0, 1, 0, "PC 17599", 71.28, "C85", "C"),
        (3, 1, 3, "Heikkinen, Miss. Laina", "female", None, 0, 0, "STON/O2. 3101282", 7.925, None, None)
    ]

def test_input_data_structure(input_schema, input_data):
    """Test input data structure"""
    # Verify schema fields
    assert len(input_schema.fields) == 12
    assert [field.name for field in input_schema.fields] == [
        "PassengerId", "Survived", "Pclass", "Name", "Sex", "Age",
        "SibSp", "Parch", "Ticket", "Fare", "Cabin", "Embarked"
    ]
    
    # Verify data structure
    assert len(input_data) == 3
    assert all(len(row) == 12 for row in input_data)

def test_input_data_values(input_data):
    """Test input data values"""
    # Check PassengerId is sequential
    passenger_ids = [row[0] for row in input_data]
    assert passenger_ids == [1, 2, 3]
    
    # Check Survived values are binary
    survived_values = [row[1] for row in input_data]
    assert all(val in [0, 1] for val in survived_values)
    
    # Check Pclass values are valid
    pclass_values = [row[2] for row in input_data]
    assert all(val in [1, 2, 3] for val in pclass_values)
    
    # Check Sex values are valid
    sex_values = [row[4] for row in input_data]
    assert all(val in ["male", "female"] for val in sex_values)

def test_missing_values(input_data):
    """Test handling of missing values"""
    # Check for None values in Age
    age_values = [row[5] for row in input_data]
    assert any(val is None for val in age_values)
    
    # Check for None values in Cabin
    cabin_values = [row[10] for row in input_data]
    assert any(val is None for val in cabin_values)
    
    # Check for None values in Embarked
    embarked_values = [row[11] for row in input_data]
    assert any(val is None for val in embarked_values)

def test_name_formats(input_data):
    """Test name formats and title extraction"""
    names = [row[3] for row in input_data]
    
    # Check name format (Last, Title. First)
    assert all("," in name and "." in name for name in names)
    
    # Check titles
    titles = ["Mr", "Mrs", "Miss"]
    assert all(any(title in name for title in titles) for name in names)

def test_numeric_ranges(input_data):
    """Test numeric value ranges"""
    # Check Age range
    ages = [row[5] for row in input_data if row[5] is not None]
    assert all(0 <= age <= 100 for age in ages)
    
    # Check Fare range
    fares = [row[9] for row in input_data]
    assert all(fare >= 0 for fare in fares)
    
    # Check SibSp and Parch are non-negative
    sibsp_values = [row[6] for row in input_data]
    parch_values = [row[7] for row in input_data]
    assert all(val >= 0 for val in sibsp_values + parch_values)
