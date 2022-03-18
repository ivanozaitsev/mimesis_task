# Data Generator
### Using Mimesis and other Python packages I created script that generates fake datasets.
#### You will find three .py files here: 
#### - main.py consists class DataGenerator with different methods that creates datasets with fake data and moves them into MySQL DB;
#### - insertion.py executes DataGenerator methods;
#### - pandas_part.py reads data from MySQL DB into dataframes, makes some transformation and puts data into .csv and .parquet files.  
### To start the project you will need:
#### 1. Run main.py script. It creates tables inside MySQL DB, generates 3 dataframes and inserts them into MySQL DB. 
#### 2. Run pandas_part.py script. It reads data from MySQL tables into dataframes, makes some transformations and joins, and save it as .parquet and .csv files.
