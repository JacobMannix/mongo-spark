# This script connects to the MongoDB collection, and runs the pyspark code on the contents

# import dependencies
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import regexp_replace
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import HashingTF
from pyspark.ml.feature import IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# create a pyspark connection to MongoDB
conf = SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1")
sc = SparkContext(conf=conf)

# connect pyspark to the specific collection
spark = SparkSession.builder.appName("myApp") \
.config("spark.mongodb.input.uri", "mongodb://localhost:27017/Twitter.Tweets") \
.config("spark.mongodb.output.uri", "mongodb://localhost:27017/Twitter.Tweets") \
.getOrCreate()

# create a pyspark dataframe from the collection
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

# change numeric columns to double
df = df.withColumn("favorite_count", df['favorite_count'].cast(DoubleType()))
df = df.withColumn("retweet_count", df['retweet_count'].cast(DoubleType()))

# clean up text column
REGEX ='[\.,[0-9]:\'@"!\\-]'
df = df.withColumn('text',regexp_replace(df['text'],REGEX," "))

# tokenized the tweets
df = Tokenizer(inputCol='text',outputCol='tokens').transform(df)

# remove stop words, and set an output column
stopwords = StopWordsRemover()
stopwords = stopwords.setInputCol('tokens').setOutputCol('words')
df = stopwords.transform(df)

# hash the tokens
hasher = HashingTF(inputCol="words", outputCol="hash", numFeatures=32)
df = hasher.transform(df)

# create an IDF based on the hashed tokens
df = IDF(inputCol="hash", outputCol="features").fit(df).transform(df)

# recode favorite count, greater than 100 as 1, less than 100 as 0, call it label
df = df.withColumn('label', (df['favorite_count'] >= 100).cast('integer'))

# select only the needed columns
df = df.select('text','label','features')

# create the test/train split
df_train, df_test = df.randomSplit([0.8, 0.2], seed=23)

# run logistic regression on the training set
lr = LogisticRegression()
lr_model = lr.fit(df_train)

# create predictions based on the test set
predictions = lr_model.transform(df_test)

# group the predictions
predictions.groupBy('label', 'prediction').count().show()

# print the test results table, and the AUC-ROC curve area
print("########################################################################")
print("Here are the results")
evaluator = BinaryClassificationEvaluator(rawPredictionCol='prediction', labelCol='label')
print(predictions.select("label","rawPrediction","prediction","probability").show())
print("The area under ROC for test set is {}".format(evaluator.evaluate(predictions)))
print("########################################################################")
