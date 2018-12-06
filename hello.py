#! /usr/bin/env python3

import flask
import json
from flask import Flask 
from flask import request 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType, StringType, StructType, BooleanType, ArrayType, DoubleType
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml import PipelineModel





#initialize the server 
app = flask.Flask(__name__)

#define a /hello route for only post requests
def init_spark_context():
	conf = SparkConf().setAppName("github-linkedin-server").set("spark.jars","file:////opt/spark/spark-2.3.1-bin-hadoop2.7/jars/azure-storage-2.0.0.jar,file:////opt/spark/spark-2.3.1-bin-hadoop2.7/jars/hadoop-azure-2.7.7.jar")
	sc = SparkContext(conf=conf, pyFiles=['hello.py'])
	sc._jsc.hadoopConfiguration().set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
	sc._jsc.hadoopConfiguration().set("fs.azure.account.key.tfsmodelstorage.blob.core.windows.net", "WOrz8vS02QqLPDn5vWm6HQzpxInSxJ/yJP5eyhKVRTZ1tK86oJIYxSmK/xzUJHc+vfHhNFM5fH8kqNcr8C+bww==")

	return sc

def fudf(val):
    if val is None or val == []:
        return []
    else:
        return reduce (lambda x, y :x+y, val)

flattenUdf = F.udf(fudf, ArrayType(StringType()))


mergeCols = F.udf(lambda x,y: [] if (x is None or y is None) else x+y, ArrayType(StringType()))

def uniqueId(value):
    return 'g'+(repr(value))

def tostring(value):
    return repr(value)

uniqueId_udf = F.udf(uniqueId, StringType())
tostring_udf = F.udf(tostring, StringType())

isEmpty = F.udf(lambda x: x == None or len(x) == 0, BooleanType())
isNotEmpty = F.udf(lambda x: x != None and len(x) != 0, BooleanType())

@app.route('/hello',methods =['POST'])
def classification():
	#grabs the Linkedin data tagged as 'linkedin'
	linkedin = request.get_json()['linkedin'] # dictionary object
	linkedin = json.dumps(linkedin) # convert the list object into a json-format string 
	github = request.get_json()['github']
	github = json.dumps(github)

	sc = init_spark_context()
	sqlContext = SQLContext(sc)


	rddLink = sc.parallelize([linkedin])
	rddGit = sc.parallelize([github])

	link = sqlContext.read.json(rddLink)
	git = sqlContext.read.json(rddGit)

	link = link.withColumnRenamed('linkedin','linkedin_id')
	joined = git.join(link, ["linkedin_id"])
	joined = joined.withColumnRenamed('linkedin_id','idLink').withColumnRenamed('gid','idGit')
	git.persist()
	link.persist()
	joined.persist()

    # Could re-write this part of pre-processing into a separate function 
	git = git.withColumn('git_lang', flattenUdf(git.repos.lang))
	git = git.withColumn('_skills', mergeCols(git.git_lang, F.col('repos.name')))
	git = git.withColumn("git_lang", F.when(isNotEmpty(F.col('git_lang')),F.concat_ws(',', F.col('git_lang'))).otherwise(F.lit('')))
	git = git.withColumn("_skills", F.when(isNotEmpty(F.col('_skills')),F.concat_ws(',', F.col('_skills'))).otherwise(F.lit('')))
	link = link.withColumn('skills_project', mergeCols(F.col('skills'), F.col('projects.title')))
	link = link.withColumn('_skills', mergeCols(link.skills_project, F.col('publications.title')))
	link = link.withColumn('edu_exp', mergeCols(F.col('education.name'), F.col('experience.organization')))
	link = link.withColumn('_edu_exp', F.when(isNotEmpty(F.col('edu_exp')),F.concat_ws(',', F.col('edu_exp'))).otherwise(F.lit('')))
	link = link.withColumn("skills", F.when(isNotEmpty(F.col('skills')),F.concat_ws(',', F.col('skills'))).otherwise(F.lit('')))
	link = link.withColumn("_skills", F.when(isNotEmpty(F.col('_skills')),F.concat_ws(',', F.col('_skills'))).otherwise(F.lit('')))

	git = git.withColumn('_pro_pub_title', F.when(isNotEmpty(F.col('repos.name')), F.concat_ws(',', F.col('repos.name'))).otherwise(F.lit('')))
	git = git.withColumn('_pro_pub_desc', F.when(isNotEmpty(F.col('repos.description')), F.concat_ws(',', F.col('repos.description'))).otherwise(F.lit('')))

	link = link.withColumn('pro_pub_title', mergeCols(F.col('projects.title'), F.col('publications.title')))
	link = link.withColumn('pro_pub_desc', mergeCols(F.col('projects.description'), F.col('publications.summary')))
	link = link.withColumn('_pro_pub_title', F.when(isNotEmpty(F.col('pro_pub_title')), F.concat_ws(',', F.col('pro_pub_title'))).otherwise(F.lit('')))
	link = link.withColumn('_pro_pub_desc', F.when(isNotEmpty(F.col('pro_pub_desc')), F.concat_ws(',', F.col('pro_pub_desc'))).otherwise(F.lit('')))

	git = git.withColumn('summary',F.when((F.col('bio').isNotNull() | F.col('git_email').isNotNull()| F.col('repos.name').isNotNull()|F.col('git_company').isNotNull()), F.concat(git.bio,F.lit(' '),git.git_email, F.lit(' '), git._pro_pub_title, F.lit(' '), git.git_company)).otherwise(F.lit('')))
	link = link.withColumn("exp_desc", F.when(isNotEmpty(F.col('experience.description')),F.concat_ws(',', F.col('experience.description'))).otherwise(F.lit('')))

	git = git.withColumnRenamed('gid','id')

	link = link.withColumnRenamed('linkedin_id','id').withColumnRenamed('headline','bio').withColumnRenamed('skills', 'git_lang')

	git = git.withColumn('id', uniqueId_udf(git.id))

	link = link.withColumn('id', tostring_udf(link.id))

	model_TFIDF =PipelineModel.load("wasb://test-container@tfsmodelstorage.blob.core.windows.net/model_tfidf0913")



	test = link.count()






	#sends a hello back to the requester
	return ("test "+ str(test))#"github "+ github[0]['git_name']+"\n"+)

#@app.route('/users/<string:username>')
#def index(username = None):
#	return("Hello {}!".format(username))