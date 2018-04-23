from pyspark import SparkConf, SparkContext
import sys

APP_NAME = 'TEST'
NAME_NODE_HOST = 'hdfs://harrisburg:49110'
COUNTRY_CODES = '/cs455/country_codes.csv'

INDEXES = {
    'ACTOR1': 5,
    'ACTOR2': 15,
    'TONE': 34
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
    return columns.map(lambda x: (x[INDEXES['ACTOR2']][0:3], (1, float(x[INDEXES['TONE']]))))


def pull_data(sc, in_path, out_path):
    codes = load_codes(sc)
    file = sc.textFile(in_path)

    composite = retrieve_actors(file, codes)

    sums = composite.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    means = sums.mapValues(lambda x: x[0]/x[1])
    means.saveAsTextFile(out_path)


def main():
    if sys.argv.__len__() < 3:
        print("Usage: Actor1(3 Char ISO code) InputPath OutputPath")
        return

    conf = SparkConf().setAppName(APP_NAME)
    sc = SparkContext(conf=conf)
    in_path = sys.argv[2]
    out_path = sys.argv[3]
    clear_directory(sc, out_path)

    pull_data(sc, in_path, out_path)


if __name__ == '__main__':
    main()
