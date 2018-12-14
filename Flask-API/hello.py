#! /usr/bin/env python3

import flask
import json
from flask import Flask 
from flask import request 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField,FloatType, StringType, DoubleType, IntegerType, ArrayType, BooleanType
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from jellyfish import levenshtein_distance as ld
from jellyfish import jaro_winkler

from featureExtraction import FeatureExtraction 

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




#initialize the server 
app = flask.Flask(__name__)

#define a /hello route for only post requests
def init_spark_context():
    conf = SparkConf().setAppName("github-linkedin-server").setMaster("spark://10.8.0.14:7077").set("spark.driver.maxResultSize", "3.5g").set("spark.executor.extraClassPath", "/opt/spark/spark-2.3.1-bin-hadoop2.7/jars/*.jar").set("spark.memory.offHeap.enabled","true").set("spark.memory.offHeap.size","5g").set("spark.jars","file:////opt/spark/spark-2.3.1-bin-hadoop2.7/jars/azure-storage-2.0.0.jar,file:////opt/spark/spark-2.3.1-bin-hadoop2.7/jars/hadoop-azure-2.7.7.jar").set("spark.network.timeout", "360s").set("spark.executor.heartbeatInterval","15s").set("spark.sql.catalogImplementation","hive").set("spark.dynamicAllocation.enabled","false").set("spark.driver.memory","12g").set("spark.executor.memory","26g").set("spark.driver.maxResultSize","4g").set("spark.executor.cores","3").set("spark.jars","file:////opt/spark/spark-2.3.1-bin-hadoop2.7/jars/azure-storage-2.0.0.jar,file:////opt/spark/spark-2.3.1-bin-hadoop2.7/jars/hadoop-azure-2.7.7.jar").set("spark.submit.deployMode", "client").set("spark.driver.port","40015").set("spark.blockManager.port","40045").set("spark.driver.blockManager.port","40075").set("spark.worker.port", "40105").set("spark.shuffle.service.port", "40135")
   # spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    #sc = spark.sparkContext
    sc = SparkContext(conf=conf, pyFiles=['featureExtraction.py'])
    sc._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb")
    sc._jsc.hadoopConfiguration().set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.azure.account.key.tfsmodelstorage.blob.core.windows.net", "WOrz8vS02QqLPDn5vWm6HQzpxInSxJ/yJP5eyhKVRTZ1tK86oJIYxSmK/xzUJHc+vfHhNFM5fH8kqNcr8C+bww==")
    #sc = spark.sparkContext
    return sc


def similarities(linkedin_id, gid, lookupTable):
          X, Y = lookupTable.value[linkedin_id], lookupTable.value[gid]

          def _cosine_similarity(X, Y):
              denom = X.norm(2) * Y.norm(2)
              if denom == 0.0:
                  return -1.0
              else:
                 return X.dot(Y)*1. / float(denom)

          gitlang_simi = _cosine_similarity(X['git_langIDF'], Y['git_langIDF'])
          skill_simi = _cosine_similarity(X['_skillsIDF'], Y['_skillsIDF'])
          edu_exp_simi = _cosine_similarity(X['_edu_expIDF'], Y['_edu_expIDF'])
          bio_simi = _cosine_similarity(X['bioIDF'], Y['bioIDF'])
          exp_desc_simi = _cosine_similarity(X['exp_descIDF'], Y['exp_descIDF'])
          summary_simi = _cosine_similarity(X['summaryIDF'], Y['summaryIDF'])
          pro_pub_title_simi = _cosine_similarity(X['_pro_pub_titleIDF'], Y['_pro_pub_titleIDF'])
          pro_pub_desc_simi = _cosine_similarity(X['_pro_pub_descIDF'], Y['_pro_pub_descIDF'])
          skill_simi = _cosine_similarity(X['_skillsIDF'], Y['_skillsIDF'])
          edu_exp_simi = _cosine_similarity(X['_edu_expIDF'], Y['_edu_expIDF'])
          bio_simi = _cosine_similarity(X['bioIDF'], Y['bioIDF'])
          exp_desc_simi = _cosine_similarity(X['exp_descIDF'], Y['exp_descIDF'])
          summary_simi = _cosine_similarity(X['summaryIDF'], Y['summaryIDF'])
          pro_pub_title_simi = _cosine_similarity(X['_pro_pub_titleIDF'], Y['_pro_pub_titleIDF'])
          pro_pub_desc_simi = _cosine_similarity(X['_pro_pub_descIDF'], Y['_pro_pub_descIDF'])
          return gitlang_simi, skill_simi, edu_exp_simi, bio_simi, exp_desc_simi, summary_simi, pro_pub_title_simi, pro_pub_desc_simi    



@app.route('/hello',methods =['POST'])
def classification():
	#grabs the Linkedin data tagged as 'linkedin'
	linkedin = request.get_json()['linkedin'] # dictionary object
        linkedin = json.dumps(linkedin) # convert the list object into a json-format string 
	github = request.get_json()['github']
	github = json.dumps(github)


        GitSchema = StructType([
            StructField("repos",ArrayType(StructType([StructField("lang", ArrayType(StringType())),StructField("isFork", BooleanType(), False),StructField("description", StringType(), False),StructField("name", StringType(), False),StructField("readme", StringType(), False), StructField("url", StringType(), False)]))),
            StructField("git_org", ArrayType(StringType())),
            StructField("git_name", StringType()),
            StructField("bio", StringType()),
            StructField("git_location", StringType()),
            StructField("git_company", StringType()),
            StructField("gid", IntegerType()),
            StructField("github_url", StringType()),
            StructField("git_email", StringType()),
            StructField("git_blog", StringType()),      
            StructField("git_login",StringType()),
            StructField("git_websiteUrl", StringType()),
            StructField("linkedin_id", IntegerType())])

        LinkSchema = StructType([
            StructField("education",ArrayType(StructType([StructField("major", StringType(), False),StructField("name", StringType(), False), StructField("summary", StringType(), False)]))),
            StructField("experience", ArrayType(StructType([StructField("description", StringType(), False),StructField("organization", StringType(), False), StructField("title", StringType(), False)]))),
            StructField("full_name", StringType()),
            StructField("headline", StringType()),
            StructField("industry", StringType()),
            StructField("interests", ArrayType(StringType())),
            StructField("linkedin", IntegerType()),
            StructField("linkedin_url", StringType()),
            StructField("location", StringType()),
            StructField("projects", ArrayType(StructType([StructField("title", StringType(), False), StructField("description", StringType(), False)]))),
            StructField("publications",ArrayType(StructType([StructField("name", StringType(), False), StructField("summary", StringType(), False)]))),
            StructField("skills", ArrayType(StringType())),
            StructField("summary", StringType()),
            StructField("websites",  ArrayType(StructType([StructField("url", StringType(), False), StructField("description", StringType(), False)])))    
   ])



	sc = init_spark_context()
    #sc = spark.sparkContext
	sqlContext = SQLContext(sc)

        rddLink = sc.parallelize([linkedin])
	rddGit = sc.parallelize([github])
        logger.info("rddLink loaded!")
    #logger.info(linkedin[0:7000])
    #logger.info(rddLink)
    #logger.info(sc._conf.getAll())
        link = sqlContext.read.json(rddLink, LinkSchema)
        git = sqlContext.read.json(rddGit, GitSchema)

	git.persist()
	link.persist()

    
	global feature_extraction

	feature_extraction = FeatureExtraction(sc,sqlContext, link, git)

	pairId = feature_extraction.joined.select('linkedin_id','gid')
        logger.info(pairId.show())
        lookupTable = feature_extraction.lookupTable
	pairPersonDF = pairId.rdd.map(lambda x: x + similarities(x[0], x[1], lookupTable))
        logger.info(pairId.rdd.map(lambda x: x + similarities('213964086','g17517', lookupTable)))
        logger.info(similarities('213964086','g17517', feature_extraction.lookupTable))
	measureMapping = sqlContext.createDataFrame(pairPersonDF.map(lambda x: Row(linkedin_id=x[0], 
                                                                          gid=x[1], 
                                                                          gitlang_simi=float(x[2]),
                                                                          skill_simi = float(x[3]),
                                                                          edu_exp_simi = float(x[4]),
                                                                          bio_simi = float(x[5]),
                                                                          exp_desc_simi = float(x[6]),
                                                                          summary_simi = float(x[7]),
                                                                          pro_pub_title_simi = float(x[8]),
                                                                          pro_pub_desc_simi= float(x[9]))))
	
	df = feature_extraction.df
        df['name_leven'] = df.apply(lambda row: ld(row['git_name'], row['full_name']), axis = 1)
        df['name_dmetaphone'] = df.apply(lambda row: feature_extraction.doublemetaphone(row['git_name'], row['full_name']), axis = 1)
        df['name_jw'] = df.apply(lambda row: jaro_winkler(row['git_name'], row['full_name']), axis = 1)
        df['name_fuzz'] = df.apply(lambda row: feature_extraction.fuzz_sort(row['git_name'], row['full_name']), axis = 1)

	df['git_company'] = df.apply(lambda row: feature_extraction.preprocess_company(row['git_company']), axis = 1)
	for column in ['git_name','full_name']:
           df[column] = df.apply(lambda row: feature_extraction.preprocess(row[column]), axis = 1)

        df['login_fuzz'] = df.apply(lambda row: feature_extraction.fuzz_sort(row['git_login'], row['full_name']), axis = 1)
        df['login_jw'] = df.apply(lambda row: jaro_winkler(row['git_login'], row['full_name']), axis = 1)
        df['location_fuzz_sort'] = df.apply(lambda row: feature_extraction.fuzz_sort(row['git_location'], row['location']), axis = 1)
        df['school_company'] = df.apply(lambda row: feature_extraction.fuzz_sort(row['git_company'], row['edu_name']), axis = 1)
        df['company_company'] = df.apply(lambda row: feature_extraction.fuzz_sort(row['git_company'], row['exp_org']), axis = 1)
        df['linkedin_gitblog'] = df.apply(lambda row: feature_extraction.leven_list(row['linkedin_url'], row['git_blog']), axis = 1)
        df['linkedin_gitweb'] = df.apply(lambda row: feature_extraction.leven_list(row['linkedin_url'], row['git_websiteUrl']), axis = 1)
        df['linkweb_gitblog'] = df.apply(lambda row: feature_extraction.leven_list(row['website'], row['git_blog']), axis = 1)
        df['linkweb_gitweb'] = df.apply(lambda row: feature_extraction.leven_list(row['website'], row['git_websiteUrl']), axis = 1)
        df['linkweb_github'] = df.apply(lambda row: feature_extraction.leven_list(row['website'], row['github_url']), axis = 1)

        DFschema = StructType([
             StructField("linkedin_id", StringType()),
             StructField("gid", StringType()),
             StructField("count", IntegerType()),
             StructField("name_leven", IntegerType()),
             StructField("name_dmetaphone", FloatType()),
             StructField("name_jw", FloatType()),
             StructField("name_fuzz", IntegerType()),
             StructField("login_fuzz", IntegerType()),
             StructField("login_jw", FloatType()),
             StructField("location_fuzz_sort", IntegerType()),
             StructField("school_company", IntegerType()),
             StructField("company_company", IntegerType()),
             StructField("linkedin_gitblog", FloatType()),
             StructField("linkedin_gitweb", FloatType()),
             StructField("linkweb_gitblog", FloatType()),
             StructField("linkweb_gitweb", FloatType()),
             StructField("linkweb_github", FloatType())
       ])
        dfSpark = sqlContext.createDataFrame(df[['linkedin_id','gid','count','name_leven','name_dmetaphone','name_jw','name_fuzz','login_fuzz','login_jw','location_fuzz_sort','school_company','company_company','linkedin_gitblog','linkedin_gitweb','linkweb_gitblog','linkweb_gitweb','linkweb_github']], DFschema)

        feature = dfSpark.join(measureMapping, ['linkedin_id','gid'])

        logger.info(feature.show())

	fill_values = {'bio_simi': -0.7676835240063782,'company_company': 13.027269554813302,'count': 69.97936515849672,'edu_exp_simi': -0.7674423412298906,'exp_desc_simi': -0.4794768606439826,'gitlang_simi': -0.36508407687661887,'linkedin_gitblog': 0.6704111164342388,'linkedin_gitweb': 0.6704111164342388,'linkweb_gitblog': 0.5065308678618619,'linkweb_github': 0.584223292437445,'linkweb_gitweb': 0.5065308678618619,'location_fuzz_sort': 34.35918477770764,'login_fuzz': 49.54616025581677,'login_jw': 0.638835025212626,'name_dmetaphone': 0.7347467665413555,'name_fuzz': 76.757491115495,'name_jw': 0.8106706649850334,'name_leven': 4.463146574747353,'pro_pub_desc_simi': -0.7593587783269232,'pro_pub_title_simi': -0.901583569164877,'school_company': 8.18176379679944, 'skill_simi': -0.29417421426755436, 'summary_simi': -0.9685668356171937}
	feature_f = feature.na.fill(fill_values).withColumnRenamed('match','label')

	assembler = VectorAssembler(inputCols=feature_f.columns[3:], outputCol="features")
	feature_assem = assembler.transform(feature_f)

	testRF= RandomForestClassificationModel.load("wasb://test-container@tfsmodelstorage.blob.core.windows.net/rfModel0914")

	rfPredict = testRF.transform(feature_assem).select('linkedin_id','gid','prediction').show()#.toPandas()
        name_list = feature_extraction.df[['linkedin_id','gid','git_login']]
        result = name_list.join(rfPredict, on = ['linkedin_id','gid'], how = 'outer')
    

	#positive = positive.toPandas()
        logger.info(result)





      






	test = link.count()


        return ("hello, "+ linkedin[0]+"\n"+"test "+str(test)+"\n")


if __name__ == "__main__":
	print(("* Loading models and Flask starting server..."
		"please wait until server has fully started"))
	app.run(host='0.0.0.0', port=5002)

