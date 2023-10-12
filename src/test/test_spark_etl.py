import pytest
from pyspark.sql import SparkSession
from main.spark_etl import transfrom_hotel_df, load_weather, transform_weather

# Import the mocker fixture from pytest-mock
@pytest.mark.usefixtures("spark_session")
def test_load_hotels(spark_session, mocker):
    # Define a sample hotels DataFrame with columns: Id, Name, Country, City, Address, Latitude, Longitude
    sample_hotels_data = [
    (101, "Hotel XYZ", "US", "New York", "123 Broadway Ave", 40.7128, -74.0060),
    (102, "Luxury Inn", "UK", "London", "456 Park Lane", 51.5074, -0.1278),
    (103, "Beach Resort", "FR", "Nice", "789 Ocean View Dr", 43.7102, 7.2620),
    (104, "Mountain Lodge", "CA", "Vancouver", "567 Forest Rd", 49.2827, -123.1207),
        # Add more sample hotel data rows as needed
    ]
    #expected_geohashes are taken from http://geohash.co/
    expected_geohashes = ['dr5r', 'gcpv', 'spv0', 'c2b2']
    hotels_df = spark_session.createDataFrame(sample_hotels_data, schema=["Id", "Name", "Country", "City", "Address", "Latitude", "Longitude"])

    # Mock the load_hotels function to return the sample hotels DataFrame
    mocker.patch("main.spark_etl.load_hotels", return_value=hotels_df)

    # Call the load_hotels function
    result_df = transfrom_hotel_df(hotels_df)
    geohashes = [row.Geohash for row in result_df.select("Geohash").collect()]
    assert geohashes == expected_geohashes

    # Check if the result DataFrame has the expected columns
    assert "Geohash" in result_df.columns
    assert "Id" in result_df.columns
    assert "Name" in result_df.columns
    assert "Country" in result_df.columns
    assert "City" in result_df.columns
    assert "Address" in result_df.columns
    assert "avg_latitude" in result_df.columns
    assert "avg_longitude" in result_df.columns
    # Add more assertions to check the content of the result DataFrame

def test_load_weather(spark_session, tmpdir):
    # Create a temporary directory with Parquet files for testing
    parquet_dir = tmpdir.mkdir("parquet_data")

    # Generate some sample Parquet files (replace this with actual data)
    sample_data = [
        (40.7128, -74.0060, 75.0, 23.0, "2023-08-23"),
        (51.5074, -0.1278, 80.0, 26.0, "2023-08-24"),
        (51.5274, -0.1298, 80.0, 26.0, "2023-08-24"),
        # Add more sample weather data rows as needed
    ]
    sample_columns = ["lat", "lng", "avg_tmpr_f", "avg_tmpr_c", "wthr_date"]

    for i, data in enumerate(sample_data):
        spark_session.createDataFrame([data], sample_columns).write.parquet(
            f"{parquet_dir}/sample_weather_{i}.parquet"
        )

    # Set the 'weatherpath' environment variable to the temporary directory path
    import os

    os.environ["weatherpath"] = str(parquet_dir)

    # Call the load_weather function
    result_df = load_weather(spark_session)
    result_df = transform_weather(result_df)

    # Assertions
    
    assert "Geohash" in result_df.columns
    assert "lat" in result_df.columns
    assert "lng" in result_df.columns
    assert "avg_tmpr_f" in result_df.columns
    assert "avg_tmpr_c" in result_df.columns
    assert "wthr_date" in result_df.columns
    
