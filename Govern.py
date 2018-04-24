from datetime import datetime

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import input_file_name 
import sys

APP_NAME = 'TEST'
NAME_NODE_HOST = 'hdfs://dover:30221'
COUNTRY_CODES = '/cs455/country_codes.csv'

INDEXES = {
    'ACTOR1': 5,
    'ACTOR2': 15,
    'TONE': 34,
    'FILE': 58
}

def clear_directory(sc, outPath):
    uri = sc._gateway.jvm.java.net.URI
    path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    file_system = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = file_system.get(uri(NAME_NODE_HOST), sc._jsc.hadoopConfiguration())

    fs.delete(path(outPath), True)


def load_codes(sc):
    file = sc.textFile(COUNTRY_CODES)
    splits = file.map(lambda x: x.split(','))
    no_header = splits.filter(lambda x: x[0] != '"Global Code"' and x[10] != '""')
    codes = no_header.map(lambda x: x[10].replace('"', ''))

    return codes.collect()


def retrieve_actors(file_rdd, codes):
    actor = sys.argv[1]
    codes.remove(actor)

    columns = file_rdd.map(lambda x: x.split('\t')).filter(
        lambda x: (x[INDEXES['ACTOR1']][0:3] == actor) and (x[INDEXES['ACTOR2']][0:3] in codes)
    )
    return columns.map(lambda x: ((x[INDEXES['ACTOR2']][0:3], x[INDEXES['FILE']]), (1, float(x[INDEXES['TONE']]))))


def pull_data(sc, in_path, out_path):
    codes = load_codes(sc)
    #file = sc.textFile(in_path)

    sql_context = SQLContext(sc)
    
    file = (sql_context.read.text(in_path) 
        .select("value", input_file_name()).rdd
        .map(lambda x: x[0] + "\t" + x[1][x[1].rfind('/') + 1:x[1].rfind('.export')]))

    composite = retrieve_actors(file, codes)

    sums = composite.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    means = sums.mapValues(lambda x: x[1]/x[0])
    return means


def graph_by_country(mean_data, out_path): 
    country_data = (mean_data.map(lambda x: (x[0][0], (int(x[0][1]), x[1])))
        .groupByKey()
        .collect())

    for country in country_data:
        graph_country(country)


def graph_country(country_data):
    image = plt.figure()    
    line_graph = image.add_subplot(111)
    
    axes = zip(*sorted(list(country_data[1])))
    
    first_day = axes[0][0]
    last_day = axes[0][-1]
    date = [x - first_day for x in axes[0]]
    mean_by_date = axes[1]

    first_day = datetime.strptime(str(first_day), '%Y%m%d').strftime('%Y-%m-%d')
    last_day = datetime.strptime(str(last_day), '%Y%m%d').strftime('%Y-%m-%d')

    line_graph.plot(date, mean_by_date)
    line_graph.title(country_data[0] + " average tone, " + first_day)
    line_graph.xlabel('Day')
    line_graph.ylabel('Mean Tone')
    #TODO: move to HDFS
    image.savefig(country_data[0] + '-' + first_day + 'to' + last_day + '.png') 
    plt.close(image)


def main():
    if sys.argv.__len__() < 3:
        print("Usage: Actor1(3 Char ISO code) InputPath OutputPath")
        return

    conf = SparkConf().setAppName(APP_NAME)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    in_path = sys.argv[2]
    out_path = sys.argv[3]
    clear_directory(sc, out_path)

    data = pull_data(sc, in_path, out_path)
    graph_by_country(data, out_path)


if __name__ == '__main__':
    main()
