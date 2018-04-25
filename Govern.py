from datetime import datetime

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import geopandas

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import input_file_name 

import os
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
    means.saveAsTextFile(out_path)
    return means


def generate_graphs(mean_data, out_path): 
    world_data = mean_data.map(lambda x: (x[0][0], (int(x[0][1]), x[1])))

    country_data = world_data.groupByKey().collect()    

    world_data = (world_data.mapValues(lambda x: (x[1], 1))
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
        .mapValues(lambda x: x[0]/x[1])).toDF().toPandas()

    if not os.path.exists('./output'):
        os.makedirs('./output')

    if not os.path.exists('./output/by_country'):
        os.makedirs('./output/by_country')

    for country in country_data:
        graph_country(country)

    graph_choropleth(world_data)


def graph_country(country_data):
    image = plt.figure()    
    line_graph = image.add_subplot(111)
    
    axes = zip(*sorted(list(country_data[1])))
    
    first_day = axes[0][0]
    last_day = axes[0][-1]
    first_day_str = datetime.strptime(str(first_day), '%Y%m%d').strftime('%Y-%m-%d')
    last_day_str = datetime.strptime(str(last_day), '%Y%m%d').strftime('%Y-%m-%d')

    start_month = first_day / 100

    date = [((x - first_day) % 100) + (31 * (x / 100 - start_month)) for x in axes[0]]
    mean_by_date = axes[1]


    line_graph.plot(date, mean_by_date)
    line_graph.set_title(country_data[0] + ' average tone, ' + first_day_str + ' to ' + last_day_str)
    line_graph.set_xlabel('Day')
    line_graph.set_ylabel('Mean Tone')
    
    image.savefig('./output/by_country/' + country_data[0] + '-' + first_day_str + 'to' + last_day_str + '.png') 
    plt.close(image)


def graph_choropleth(mean_by_country):
    map_file = './country_map/countries.shp'
    country_map = geopandas.read_file(map_file)[['ADM0_A3', 'geometry']].to_crs('+proj=robin')
    merged_data = country_map.merge(mean_by_country, left_on='ADM0_A3', right_on='_1')
    
    num_colors = 11
    color_map = 'RdBu' # Red for negative numbers, blue for positive
    title = '{} relations with other countries'.format(sys.argv[1])

    choropleth = (merged_data.dropna()
        .plot(column = '_2', cmap=color_map, figsize=(16,10), 
        scheme='equal_interval', k=num_colors, legend=True))

    choropleth.set_title(title, fontdict={'fontsize': 20})
    choropleth.set_axis_off()
    choropleth.set_xlim([-1.5e7, 1.7e7])
    choropleth.get_legend().set_bbox_to_anchor((.12, .1))
    fig = choropleth.get_figure()
    fig.savefig('./output/current_relations.png')


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
    generate_graphs(data, out_path)


if __name__ == '__main__':
    main()
