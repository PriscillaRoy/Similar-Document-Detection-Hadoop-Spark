from pyspark import SparkContext,SparkConf,SQLContext
import sys

conf = SparkConf().setAppName("part 4 text")
sc = SparkContext(conf = conf)

partfile   = sys.argv[1]
outputfile = sys.argv[2]

sqlcontext = SQLContext(sc)
RDD_Similardocs  = sc.wholeTextFiles(partfile,use_unicode=False).map(lambda (file,invertedIndex) : invertedIndex.split("\n"))
RDD_mapped = RDD_Similardocs.flatMap(lambda x : x)
RDD_processed = RDD_mapped.filter(lambda x : len(x) > 0).map(lambda x : eval(x))

file_data = RDD_processed.takeOrdered(10, key = lambda x: -x[1])

file_data = [i[0] for i in file_data]

finalFile = sc.parallelize(file_data)
finalFile.saveAsTextFile(outputfile)
finalFlie_print = finalFile.collect()
print(finalFlie_print)
