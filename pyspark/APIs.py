from pyspark import SparkConf, SparkContext
import os
os.environ["SPARK_HOME"] = "/Users/stefan/Apache_env/spark-2.2.0-bin-2.6.0-cdh5.7.0"
os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"

if __name__ == '__main__':

    conf = SparkConf().setAppName("spark").setMaster("local[2]")
    sc = SparkContext(conf=conf)

    def my_map():
        rdd1 = sc.parallelize([1, 2, 3, 4, 5])
        print(rdd1.map(lambda x:(x,1)).collect())

    def my_filter():
        rdd1 = sc.parallelize([1, 2, 3, 4, 5])
        print(rdd1.map(lambda x:x*2).filter(lambda x:x>5).collect())

    def my_flatmap():
        rdd1 = sc.parallelize(["I love WPI", "WPI love me"])
        print(rdd1.flatMap(lambda line:line.split(" ")).collect())

    def my_groupbykey():
        rdd1 = sc.parallelize(["I love WPI", "WPI love me"])
        rdd2 = rdd1.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1)).groupByKey()
        print(rdd2.collect())
        print(rdd2.map(lambda x:{x[0]:list(x[1])}).collect())

    def my_reducebykey():
        rdd1 = sc.parallelize(["I love WPI", "WPI love me love"])
        rdd2 = rdd1.flatMap(lambda line:line.split(" ")).map(lambda x:(x,1))
        print(rdd2.collect())
        print(rdd2.reduceByKey(lambda x,y:x+y).collect())
        # rdd2 group by key first to: [('I', [1]), ('love', [1,1,1]), ('WPI', [1,1]), ('me', [1])]
        # Then reduce by key count x + y
        # done

    my_reducebykey()
    sc.stop()