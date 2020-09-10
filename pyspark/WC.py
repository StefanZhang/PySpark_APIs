import sys
from pyspark import SparkConf, SparkContext

# os.environ["SPARK_HOME"] = "/Users/stefan/Apache_env/spark-2.2.0-bin-2.6.0-cdh5.7.0"
# os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"

if __name__ == '__main__':

    # Check on params
    if len(sys.argv) != 3:
        print("Usage: <input> <output>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf()
    sc = SparkContext(conf=conf)

    # Word Count
    # Split, map, reducebykey
    counts = sc.textFile(sys.argv[1])\
        .flatMap(lambda line:line.split(" "))\
        .map(lambda x:(x, 1))\
        .reduceByKey(lambda x,y:x+y)\

    # DESC order
    out = counts.map(lambda x:(x[1], x[0])).sortByKey(False).map(lambda x:(x[1], x[0]))

    out.saveAsTextFile(sys.argv[2])

    sc.stop()

    # ./ spark - submit - -master
    # local[2] - -name
    # WC / root / scripts / WC.py
    # file: // / root / data / hello.txt
    # file: // / root / data / output
