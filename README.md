# CS 455 Term Project
by Chris Westerman, Thomas Shaw, and Tanner Johnson 

## Better Governing with Data Science

Our project analyzes the GDELT event dataset to provide visualizations depicting any country's relationship with every other country in the world over a given time frame. The GDELT event dataset collects news articles from around the world in dozens of languages, collects together similar events, and assigns an average language tone score for each event in a range from -100 to 100. These tone scores give us an insight into the publicly perceived relationships between international actors. 

We use the Python Apache Spark API (Pyspark) to average the tone for a country's interactions with other countries across a series of GDELT data files. These files, generated each day, are typically 50 MB to 100 MB in size. Using our experimental setup, processing a month's worth of data takes a little over a minute. Using Matplotlib, we generate a choropleth to give a global overview of a primary actor's international relations, as well as individual line graphs for each secondary actor. For more information on the program and its results, please refer to the PDF report at the top level of this repo. 

## Project dependencies (available through pip)
- pyspark
- pandas
- geopands
- pysal
- matplotlib

## Running
This software uses Pyspark and should be submitted using Spark's spark-submit script. The program name and arguments are:

``` Pyspark API for Apache Spark to 
Govern.py [primary actor] [input directory] [output directory]
```

Where primary actor is the country whose relationships should be analyzed ("USA", for example), input directory is a folder containing GDELT event CSV files, and output directory is where the analysis data should be stored on HDFS. Images will be stored to the local disk of the Spark driver in a subdirectory named "output". 

## Example output

### USA's global relations
Global averages from March 22nd, 2018 to April 22nd, 2018:
![USA relations choropleth](https://raw.githubusercontent.com/tjohns0n/GDELT-Visualization/master/sample_output/current_relations.png)

### USA's relationship with Russia
Taken from the same time period as the above global relation graph. 
![USA-Russia relationship](https://raw.githubusercontent.com/tjohns0n/GDELT-Visualization/master/sample_output/RUS-2018-03-22to2018-04-22.png)
