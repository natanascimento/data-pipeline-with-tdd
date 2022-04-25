import pytest
from pyspark.sql import SparkSession

from app.transformations.distance_transformer import calculate_distance, compute_distance

@pytest.fixture
def spark_session():
  spark = SparkSession.builder.appName("UnitTests").getOrCreate()
  return spark

@pytest.fixture
def fake_dataset(spark_session):
  data = [(1, 40.71534825, -73.96024116, 40.72311651, -73.95212324),
          (2, 42.71534825, -73.96024116, 42.72311651, -73.95212324)]
  columns = ["id","start_station_latitude","start_station_longitude","end_station_latitude","end_station_longitude"]
  
  dataset = spark_session.createDataFrame(data, columns)
  return dataset


def test_calculate_distance_when_parameters_is_none():
  
  start_latitude = None
  start_longitude = -73.96024116
  end_latitude = 40.72311651
  end_longitude = -73.95212324

  with pytest.raises(ValueError) as exception:
    calculate_distance(start_latitude, start_longitude, 
                      end_latitude, end_longitude)
  
  assert "Parameters must be required" in str(exception.value)
  
def test_calculate_distance_when_has_correct_parameters():
  
  start_latitude = 40.71534825
  start_longitude = -73.96024116
  end_latitude = 40.72311651
  end_longitude = -73.95212324
  
  distance = calculate_distance(start_latitude, start_longitude, 
                                end_latitude, end_longitude)
  
  assert distance == 0.684692

def test_compute_distance_when_has_correct_parameters(spark_session, fake_dataset):
  
  distance1 = 0.684692
  distance2 = 0.67668
  
  dataset = compute_distance(spark_session, fake_dataset)
  
  rows = dataset.collect()
  
  assert "id" in dataset.columns
  assert "start_station_latitude" in dataset.columns
  assert "start_station_longitude" in dataset.columns
  assert "end_station_latitude" in dataset.columns
  assert "end_station_longitude" in dataset.columns
  assert "distance" in dataset.columns
  assert rows[0].distance == distance1
  assert rows[1].distance == distance2
  