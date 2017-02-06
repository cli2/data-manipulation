# Calculate the average stars for each business category
# Written by Dr. Yuhang Wang and Josh Gardner
'''
To run on Fladoop cluster:
spark-submit --master yarn-client --queue si618w17 --num-executors 2 --executor-memory 1g --executor-cores 2 si618w17hw5-part1_chongli.py

To get results:
hadoop fs -getmerge si618w17hw5-part1_output si618w17hw5-part1_output.tsv
'''
import json
from pyspark import SparkContext
sc = SparkContext(appName="PySparksi618w17-part1")
input_file = sc.textFile("hdfs:///var/si618w17/yelp_academic_dataset_business_updated.json")
def cat_star(data):
  cat_star_list = []
  stars = data.get('stars',None)
  buss = data.get('business_id',None)
  neighborhood = data.get('neighborhoods', None)
  city = data.get('city',None)
  review_count = data.get('review_count',None)
  if stars != None and buss != None and neighborhood != None and city != None and review_count != None:
    if neighborhood==[]:
        neighborhood=['Unknown']
    for n in neighborhood:
        if stars>=4:
            cat_star_list.append(((city, n), (1, review_count, 1)))
        else:
            cat_star_list.append(((city, n), (1, review_count, 0)))
  return cat_star_list

list_data = input_file.map(lambda line: json.loads(line)).flatMap(lambda x:cat_star(x))
numStars = list_data.map(lambda x:(x[0],x[1][2])).reduceByKey(lambda a, b: a + b)
numReviews = list_data.map(lambda x: (x[0],x[1][1])).reduceByKey(lambda a, b: a + b)
numBuss = list_data.map(lambda x: (x[0],1)).reduceByKey(lambda a, b: a + b)
joinData = numBuss.join(numReviews).fullOuterJoin(numStars)
sortedData = joinData.sortBy(lambda x:x[1][0],ascending = False)
sortedData = sortedData.sortBy(lambda x:x[0][0])
printData = sortedData.map(lambda i: (i[0][0]+'\t'+i[0][1]+'\t'+str(i[1][0][0])+'\t'+str(i[1][0][1])+'\t'+str(i[1][1])).encode('utf-8'))
printData.saveAsTextFile('si618w17hw5-part1_output')
sc.stop()
