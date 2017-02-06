import json
from pyspark import SparkContext

sc = SparkContext(appName="PySparksi618w17-part2")
input_file1 = sc.textFile("hdfs:///var/si618w17/yelp_academic_dataset_business_updated.json")
input_file2 = sc.textFile("hdfs:///var/si618w17/yelp_academic_dataset_review_updated.json")

def getReview(data):
    reviews = []
    user = data.get('user_id',None)
    buss = data.get('business_id',None)
    star = data.get('stars',None)
    if user != None and buss != None and star != None:
        reviews.append((buss,(user,star)))
    return reviews

def getCity(data):
    cities = []
    buss = data.get('business_id',None)
    city = data.get('city',None)
    if city != None and buss !=None:
        cities.append((buss,city))
    return cities

reviewData = input_file2.map(lambda line:json.loads(line)).flatMap(lambda x:getReview(x))
cityData = input_file1.map(lambda line:json.loads(line)).flatMap(lambda x:getCity(x))
distinctUser = reviewData.join(cityData).map(lambda x:(x[1][0][0],x[1][1])).distinct().map(lambda x:(x[0],1)).reduceByKey(lambda x,y:x+y)
numberOfUser = distinctUser.map(lambda x:x[1]).histogram(range(1,32))
title = 'cities,yelp users'
printAll = [title]
for i in range(len(numberOfUser[1])):
    printAll.append(str(numberOfUser[0][i]) + ',' + str(numberOfUser[1][i]))
printAll = sc.parallelize(printAll)
printAll.saveAsTextFile('hdfs:///user/chongli/si618w17hw5-part2_output')
# good reviews
goodReview = reviewData.filter(lambda x:x[1][1]>3)
distinctGoodUser = goodReview.join(cityData).map(lambda x:(x[1][0][0],x[1][1])).distinct().map(lambda x:(x[0],1)).reduceByKey(lambda x,y:x+y)
numberOfGoodUser = distinctGoodUser.map(lambda x:x[1]).histogram(range(1,26))
printGood = [title]
for i in range(len(numberOfGoodUser[1])):
    printGood.append(str(numberOfGoodUser[0][i]) + ',' + str(numberOfGoodUser[1][i]))
printGood = sc.parallelize(printGood)
printGood.saveAsTextFile('hdfs:///user/chongli/si618w17hw5-part2_output_goodreview')
# bad reviews
badReview = reviewData.filter(lambda x:x[1][1]<3)
distinctBadUser = badReview.join(cityData).map(lambda x:(x[1][0][0],x[1][1])).distinct().map(lambda x:(x[0],1)).reduceByKey(lambda x,y:x+y)
numberOfBadUser = distinctBadUser.map(lambda x:x[1]).histogram(range(1,17))
printBad = [title]
for i in range(len(numberOfBadUser[1])):
    printBad.append(str(numberOfBadUser[0][i]) + ',' + str(numberOfBadUser[1][i]))
printBad = sc.parallelize(printBad)
printBad.saveAsTextFile('hdfs:///user/chongli/si618w17hw5-part2_output_badreview')
sc.stop()
