
from pyspark import SparkContext,SparkConf,SQLContext
import sys


outputfile   = sys.argv[1]
partfile   = sys.argv[2]

def similarity_Matrix(postings):
    sim_matrix = list()
    doc_weight   = postings[1] # list of postings excluding the word
    for i in range(len(doc_weight)):
        for j in range(i+1,len(doc_weight)):
            if(len(doc_weight) == 1):
                break
            else:
                weight_i = doc_weight[i][1]
                weight_j = doc_weight[j][1]
                sim = ((doc_weight[i][0],doc_weight[j][0]),weight_i*weight_j)
                sim_matrix.append(sim)
    return sim_matrix


conf = SparkConf().setAppName("part c3")
sc = SparkContext(conf = conf)

sqlcontext = SQLContext(sc)
sqlcontext.setConf("spark.sql.parquet.compression.codec", "uncompressed")

df = sqlcontext.read.parquet(partfile)
RDD_InvertedIndex = df.rdd


SimilarData = RDD_InvertedIndex.map(similarity_Matrix).flatMap(lambda x : x)
SimilarData_reduced = SimilarData.reduceByKey(lambda x,y : x + y)
SimilarData_sorted = SimilarData_reduced.sortBy(lambda sim : sim[1],ascending = False)

fileData_DF = SimilarData_sorted.toDF()

fileData_DF.write.parquet(outputfile)
#SimilarData_sorted.saveAsTextFile(outputfile)

