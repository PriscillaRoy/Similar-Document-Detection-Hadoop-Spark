from pyspark import SparkContext,SparkConf,SQLContext
import sys

conf = SparkConf().setAppName("part 4 avro")
sc = SparkContext(conf = conf)

partfile   = sys.argv[1]
outputfile = sys.argv[2]

sqlcontext = SQLContext(sc)
sqlcontext.setConf("spark.sql.avro.compression.codec","uncompressed")
df = sqlcontext.read.format("com.databricks.spark.avro").load(partfile)
#Converting the data-frame to a hadoop usable RDD
RDD_Similardocs = df.rdd

file_data = RDD_Similardocs.takeOrdered(10, key = lambda x: -x[1])

file_data = [i[0] for i in file_data]

finalFile = sc.parallelize(file_data)
finalFile.saveAsTextFile(outputfile)
finalFlie_print = finalFile.collect()
print(finalFlie_print)
