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

    def my_sortbykey():
        rdd1 = sc.parallelize(["I love WPI", "WPI love me love"])
        rdd2 = rdd1.flatMap(lambda line:line.split(" ")).map(lambda x:(x,1))
        rdd3 = rdd2.reduceByKey(lambda x,y:x+y)
        print(rdd3.map(lambda x:(x[1], x[0])).sortByKey(False).map(lambda x:(x[1], x[0])).collect())
        # Flip the order first, then sortbykey, last flip the order back.

    def my_union():
        rdd1 = sc.parallelize([1,2,3])
        rdd2 = sc.parallelize([4,5,6])
        print(rdd1.union(rdd2).collect())

    def my_dis():
        rdd1 = sc.parallelize([1,2,3])
        rdd2 = sc.parallelize([3,4,5])
        print(rdd1.union(rdd2).distinct().collect())



    my_dis()
    sc.stop()