
from pyspark.sql import SparkSession
import os, requests, pyspark,json
import pygeohash as pgh
from functools import reduce
from dotenv import load_dotenv
from pyspark.sql.functions import  when
from pyspark.sql.types import StringType



def start():
    spark = (SparkSession.builder
    .appName("MyLocalSparkApp")
    .config("spark.executor.memory","2g")
    .config("spark.driver.memory","5g")
    .master("local[*]") 
    .getOrCreate())
    return spark


        
def load_hotels(spark):
    """
    Load hotel data from CSV files and combine them into a single DataFrame.

    Args:
        spark (SparkSession): The SparkSession instance used for Spark operations.

    Returns:
        pyspark.sql.DataFrame: A DataFrame containing hotel data from CSV files.
    """
    # Load environment variables from a .env file
    load_dotenv()
    
    # Get the path to the directory containing hotel CSV files from environment variables
    path = os.getenv('hotelspath')
    
    # Initialize an empty list to store DataFrames
    dfs = []
    
    # Iterate through CSV files in the specified directory
    for filename in os.listdir(path):
        # Read each CSV file and configure DataFrame options
        df = (spark.read.format('csv')
            .option("header", "true")
            .option("numPartitions", 4)
            .load(os.path.join(path, filename))  # Use os.path.join to construct the file path
        )
        dfs.append(df)

    # Combine all DataFrames into a single DataFrame using unionByName
    df = reduce(pyspark.sql.dataframe.DataFrame.unionByName, dfs)
    
    return df

def add_geohash(row):
    """
    Args:
        latitude and longitude in str format
    Returns:
        geohash with precision 4
    """
    latitude = row['Latitude']
    longitude = row['Longitude']
    geohash = pgh.encode(float(latitude),float(longitude),precision = 4)

    return (row["Id"], row["Name"], row["Country"], row["City"], row["Address"], row["Latitude"], row["Longitude"], geohash)

def check_coords(row):
    """
    

    Args:
        row (pyspark.sql.Row): A row from a DataFrame containing hotel data.

    Returns:
        Tuple: A tuple containing (Id, Name, Country, City, Address, Latitude, Longitude).

    Note:
        This function uses an external API to retrieve latitude and longitude information for the 'City_Country'
        when the 'Longitude' or 'Latitude' values are missing.

    
    """
    # Load environment variables from a .env file
    load_dotenv()
    
    # Get the API key from environment variables
    api_key = os.getenv('api_key')
    
    # Extract 'City_Country' from the row
    #city_country = row['City_Country']
    placename = row['Name'] + ", " + row['Address'] + ", " + row['City'] + ", " + row['Country']
    # Use PySpark's functions to handle null values
    if row["Longitude"] is None or row["Latitude"] is None:
        # Construct the API URL with the 'City_Country' and API key
        api_url = f'https://api.opencagedata.com/geocode/v1/json?q=URI-ENCODED-{placename}&key={api_key}'
        
        # Send a GET request to the API
        response = requests.get(api_url)
        
        # Check if the API request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON response
            response_dict = json.loads(response.text)
            
            
            # Extract latitude and longitude from the response
            longitude = response_dict["results"][0]["geometry"]["lng"]
            latitude = response_dict["results"][0]["geometry"]["lat"]
            
            # Calculate the geohash based on retrieved latitude and longitude
            
            
            # Return the updated row as a tuple
            return (row["Id"], row["Name"], row["Country"], row["City"], row["Address"], latitude, longitude)
    
       
    
    # Return the row as a tuple
    return (row["Id"], row["Name"], row["Country"], row["City"], row["Address"], row["Latitude"], row["Longitude"])

def transfrom_hotel_df(df):
    """
    Transform a DataFrame containing hotel data by adding geohash information and aggregating by geohash.

    Args:
        df (pyspark.sql.DataFrame): A DataFrame containing hotel data with columns like 'Id', 'Name', 'Country', 'City', 'Address', 'Latitude', 'Longitude', and 'City_Country'.

    Returns:
        pyspark.sql.DataFrame: A transformed DataFrame aggregated by 'Geohash' with additional columns like 'avg_latitude' and 'avg_longitude'.
    """
    # Add a new column 'City_Country' by concatenating 'City' and 'Country' with a comma
    
    
    # Replace "NA" values in 'Latitude' and 'Longitude' columns with None
    df = df.withColumn("Latitude", when(df["Latitude"] == "NA", None).otherwise(df["Latitude"]))
    df = df.withColumn("Longitude", when(df["Longitude"] == "NA", None).otherwise(df["Longitude"]))
    
    # Use the 'check_coords' function to add 'latitude and longitude' to each row, where it is None
    updated_df = df.rdd.map(check_coords).toDF(["Id", "Name", "Country", "City", "Address", "Latitude", "Longitude"])
    
    

# Create a DataFrame and let PySpark infer the schema
    geo_df = updated_df.rdd.map(add_geohash).toDF(["Id", "Name", "Country", "City", "Address", "Latitude", "Longitude","Geohash"])
    
   
    

    return geo_df


    
def compute_geohash(row):
    lat = row['lat']
    lng = row['lng']
    geohash = pgh.encode(float(lat),float(lng),precision=4)
    return (row['lng'], row['lat'], row['avg_tmpr_f'], row['avg_tmpr_c'], row['wthr_date'], geohash)

def load_weather(spark):
    load_dotenv()
    #UDF to compute geohash values
    weatherpath = os.getenv('weatherpath')
    parq_dfs = []   
    #combined_df = spark.createDataFrame([], schema=None)
    # Iterate through all files and subdirectories in the given directory
    for root, dirs, files in os.walk(weatherpath):
        for file in files:
            if file.endswith(".parquet"):
                # If it's a Parquet file, read it and union with the DataFrame
                file_path = os.path.join(root, file)
                df = spark.read.parquet(file_path)
                parq_dfs.append(df)
    wdf = reduce(pyspark.sql.dataframe.DataFrame.unionByName,parq_dfs)
    
    return wdf

def transform_weather(df):
    geow_df = df.rdd.map(compute_geohash).toDF(["lng", "lat", "avg_tmpr_f", "avg_tmpr_c", "wthr_date", "Geohash"])
    
    return geow_df




def load_join(spark):
    
    hotels_df = load_hotels(spark)
    hotels_df = transfrom_hotel_df(hotels_df)
    weather_df = load_weather(spark)
    weather_df = transform_weather(weather_df)
    res = hotels_df.join(weather_df,on="Geohash",how="left")
    res.show()
    output_path = os.getcwd() + "\\result\\"
    res.coalesce(1).write.mode("overwrite").parquet(output_path)
if __name__ == "__main__":
    spark = start()
    load_join(spark)
    #spark.stop()



