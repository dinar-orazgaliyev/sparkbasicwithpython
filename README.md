<<<<<<< HEAD
# sparkbasicwithpython
=======
## Table of Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Usage](#usage)

## Introduction
This PySpark project focuses on the following key tasks:

1. Loading hotel data from CSV files.
2. Adding geohash information to hotel data.
3. Aggregating weather data.
4. Joining hotel and weather data based on geohash.
5. Saving the processed data as Parquet files.

The whole project is done locally.

## Installation

To run this project, you need to follow these installation steps:

* git clone https://github.com/dinar-orazgaliyev/sparkbasicwithpython

* Setup needed requirements into your env `pip install -r requirements.txt`
* code is in `src/main/`
* Pytests are located in `src/tests/`
* Package your artifacts
*


## Usage
* python load_join.py. That the main function that invokes the logic. First sparksession initialized by start() method
* Then load_hotels(spark) loads csv files locally from path(the path is hidden in .env file). As a result it returns hotel df
* Transform_hotel_df funciton adds geohash, and adds latitude and longitude values for "NA" rows through API.
The geohash length is 4 chars, thus additional aggregation are performed.
* load_weather(spark) function loads parquet files from local dir, and adds geohash to the loaded df.
* res df is formed by leftjoin of hotels_df and weather_df 
>>>>>>> master
