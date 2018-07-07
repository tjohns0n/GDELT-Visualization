# CS 455 Term Project
by Chris Westerman, Thomas Shaw, and Tanner Johnson 

## Better Governing with Data Science

Our project analyzes the GDELT event dataset to provide visualizations depicting any country's relationship with every other country in the world over a given time frame. 

## Project dependencies (available through pip)
- pyspark
- pandas
- geopands
- pysal
- matplotlib

## Running
This software uses Pyspark and should be submitted using Spark's spark-submit script. The program name and arguments are:

```
Govern.py [primary actor] [input directory] [output directory]
```

Where primary actor is the country whose relationships should be analyzed ("USA", for example), input directory is a folder containing GDELT event CSV files, and output directory is where the analysis data should be stored on HDFS. Images will be stored to the local disk of the Spark driver in a subdirectory named "output". 

## Example output
![USA relations choropleth](https://raw.githubusercontent.com/tjohns0n/GDELT-Visualization/master/sample_output/current_relations.png)
![USA-Russia relationship](https://raw.githubusercontent.com/tjohns0n/GDELT-Visualization/master/sample_output/RUS-2018-03-22to2018-04-22.png)
