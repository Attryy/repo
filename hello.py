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
from pyspark.sql.types import IntegerType, FloatType, StringType, StructType, BooleanType, ArrayType, DoubleType
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF, VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml import PipelineModel, pipeline
import math
import numpy as np
import name_tools
import jellyfish
import fuzzywuzzy
import phonetics
import phonetics
from name_tools import split
from jellyfish import jaro_winkler
import string
import re
from fuzzywuzzy import fuzz
import itertools
import geopy
from pyspark.sql.types import Row
from geopy import distance
from jellyfish import levenshtein_distance as ld
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




#initialize the server 
app = flask.Flask(__name__)

#define a /hello route for only post requests
def init_spark_context():
    conf = SparkConf().setAppName("github-linkedin-server").setMaster("spark://10.8.0.14:7077").set("spark.driver.maxResultSize", "3.5g").set("spark.executor.extraClassPath", "/opt/spark/spark-2.3.1-bin-hadoop2.7/jars/*.jar").set("spark.memory.offHeap.enabled","true").set("spark.memory.offHeap.size","5g").set("spark.jars","file:////opt/spark/spark-2.3.1-bin-hadoop2.7/jars/azure-storage-2.0.0.jar,file:////opt/spark/spark-2.3.1-bin-hadoop2.7/jars/hadoop-azure-2.7.7.jar").set("spark.network.timeout", "360s").set("spark.executor.heartbeatInterval","15s").set("spark.sql.catalogImplementation","hive").set("spark.dynamicAllocation.enabled","false").set("spark.driver.memory","12g").set("spark.executor.memory","26g").set("spark.driver.maxResultSize","4g").set("spark.executor.cores","3").set("spark.jars","file:////opt/spark/spark-2.3.1-bin-hadoop2.7/jars/azure-storage-2.0.0.jar,file:////opt/spark/spark-2.3.1-bin-hadoop2.7/jars/hadoop-azure-2.7.7.jar").set("spark.submit.deployMode", "client").set("spark.driver.port","40015").set("spark.blockManager.port","40045").set("spark.driver.blockManager.port","40075").set("spark.worker.port", "40105").set("spark.shuffle.service.port", "40135")
    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    #sc = SparkContext(conf=conf, pyFiles=['hello.py'])
    sc._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb")
    sc._jsc.hadoopConfiguration().set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.azure.account.key.tfsmodelstorage.blob.core.windows.net", "WOrz8vS02QqLPDn5vWm6HQzpxInSxJ/yJP5eyhKVRTZ1tK86oJIYxSmK/xzUJHc+vfHhNFM5fH8kqNcr8C+bww==")
    #sc = spark.sparkContext
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


def cosine_similarity(X, Y):
    denom = X.norm(2) * Y.norm(2)
    if denom == 0.0:
       return -1.0
    else:
       return X.dot(Y)*1. / float(denom)

def similarities(idLink, idGit, lookupTable):
    X, Y = lookupTable.value[idLink], lookupTable.value[idGit]
    gitlang_simi = cosine_similarity(X['git_langIDF'], Y['git_langIDF'])
    skill_simi = cosine_similarity(X['_skillsIDF'], Y['_skillsIDF'])
    edu_exp_simi = cosine_similarity(X['_edu_expIDF'], Y['_edu_expIDF'])
    bio_simi = cosine_similarity(X['bioIDF'], Y['bioIDF'])
    exp_desc_simi = cosine_similarity(X['exp_descIDF'], Y['exp_descIDF'])
    summary_simi = cosine_similarity(X['summaryIDF'], Y['summaryIDF'])
    pro_pub_title_simi = cosine_similarity(X['_pro_pub_titleIDF'], Y['_pro_pub_titleIDF'])
    pro_pub_desc_simi = cosine_similarity(X['_pro_pub_descIDF'], Y['_pro_pub_descIDF'])
    skill_simi = cosine_similarity(X['_skillsIDF'], Y['_skillsIDF'])
    edu_exp_simi = cosine_similarity(X['_edu_expIDF'], Y['_edu_expIDF'])
    bio_simi = cosine_similarity(X['bioIDF'], Y['bioIDF'])
    exp_desc_simi = cosine_similarity(X['exp_descIDF'], Y['exp_descIDF'])
    summary_simi = cosine_similarity(X['summaryIDF'], Y['summaryIDF'])
    pro_pub_title_simi = cosine_similarity(X['_pro_pub_titleIDF'], Y['_pro_pub_titleIDF'])
    pro_pub_desc_simi = cosine_similarity(X['_pro_pub_descIDF'], Y['_pro_pub_descIDF'])
    return gitlang_simi, skill_simi, edu_exp_simi, bio_simi, exp_desc_simi, summary_simi, pro_pub_title_simi, pro_pub_desc_simi

def preprocess(column):
    from name_tools import split
    
    if column is None:
        return ''
    else: 
        m = split(column)
        if(m[1]==''):
            full_name = m[2];
        elif(m[2]==''):
            full_name = m[1];
        else:
            full_name = ' '.join((m[1], m[2]));
        return full_name

udf_preprocess = F.udf(preprocess, StringType()) # if the function returns a string


def preprocess_company(column):
    # return a cleaned-up company name
    # first convert all words into lower case
    # remove "@", "," "ltd.", 'llc.' and 'co.' 
    
    
    
    if column is None:
        return ''
    else:
        lower = column.lower()
        result = re.sub(r"\,|@|\.", " ", lower)
        result = re.sub(' +',' ', result).strip()
        for word in ['llc','ltd','co','inc']:
            patt = re.compile('(\s*)'+word+'(\s*)')
            result = patt.sub('', result)
        return result

udf_pre_company = F.udf(preprocess_company, StringType()) # if the function returns a string

def doublemetaphone(str1, str2):
    # compute the jaro-winkler distance between first names and last names's double metaphone codes
    # equal weights are assigned to the first and last name
    # returns null when either string is null 
    
    
    if(str1 is None or str1 is None):
        return ''
    else: 
        weight1 = 0.5
        weight2 = 0.5
        
        import re
        str1 = re.sub(r'[^\x00-\x7f]',r' ',str1)   # Each char is a Unicode codepoint.
        str2 = re.sub(r'[^\x00-\x7f]',r' ',str2)
        m1 = split(str1) #(prefix, firstname, lastname, surffix)
        m2 = split(str2)  
   
        dm1 = phonetics.dmetaphone(m1[1].encode('ascii').decode('ascii'))+ phonetics.dmetaphone(m1[2].encode('ascii').decode('ascii'))
        dm2 = phonetics.dmetaphone(m2[1].encode('ascii').decode('ascii'))+ phonetics.dmetaphone(m2[2].encode('ascii').decode('ascii'))
     
    
        dm_first1 =dm1[0]+dm1[1]
        dm_last1 = dm1[2]+dm1[3]
    
        dm_first2 = dm2[0]+dm2[1]
        dm_last2 = dm2[2]+dm2[3]
        
        
        if(m2[1] ==''):
            d = jaro_winkler(unicode(dm_first1), unicode(dm_last2))*weight1+ jaro_winkler(unicode(dm_last1), unicode(dm_first2))*weight2
        elif(m1[1] == ''):
            d = jaro_winkler(unicode(dm_first2),unicode( dm_last1))*weight1+ jaro_winkler(unicode(dm_last2), unicode(dm_first1))*weight2      
        else:
            d = jaro_winkler(unicode(dm_first1),unicode(dm_first2))*weight1+ jaro_winkler(unicode(dm_last1), unicode(dm_last2))*weight2
        return round(d,5)

def jw_null(str1, str2):
   
    
    if(str1 is None or str2 is None):
        return 0.0
    else:
        d = jaro_winkler(str1, str2)
        return round(d,5)


def f(x):
    d = {}
    for i in range(len(x)):
        d[str(i)] = x[i]
    return d
    

def fuzz_sort(str1, str2):
    
    result = [];
    if(str1 ==[] or str2==[]):
        return 0
    else:
        if(isinstance(str1, list)):
            if(isinstance(str2, list)):
                for value in itertools.product(str1, str2):
                   #print(value)
                    result.append(fuzz.token_sort_ratio(preprocess_company(value[0]), preprocess_company(value[1])))
            #print("list1")
            #print(result)
                d = max(result);
            else:
                for value in enumerate(str1):
                    result.append(fuzz.token_sort_ratio(preprocess_company(value[1]), preprocess_company(str2)))
            #print("notlist2")
            #print(result)
                d = max(result)   
        elif(isinstance(str2, list)):
            for value in enumerate(str2):
                result.append(fuzz.token_sort_ratio(preprocess_company(value[1]), preprocess_company(str1)))
        #print("list2")
        #print(result)
            d = max(result);
        else:
            d = fuzz.token_sort_ratio(str1, str2)
        return(d)
            

udf_jaro_winkler = F.udf(jw_null, FloatType()) # if the function returns a float
udf_fuzz_sort = F.udf(fuzz_sort, IntegerType())
udf_dmetaphone = F.udf(doublemetaphone, FloatType())

def leven_list(str1, str2):
    result = [];
    if(str1 ==[] or str2==[] or str1 is None or str2 is None):
    	return None
    else:
    	if(isinstance(str1, list)):
    		if(isinstance(str2, list)):
    			for value in itertools.product(str1, str2):
    				result.append(ld(unicode(value[0].lower()), unicode(value[1].lower()))*1./max(len(unicode(value[0])),len(unicode(value[1]))))
    			d = min(result);
    		else:
    			for value in enumerate(str1):
    				result.append(ld(unicode(value[1].lower()), unicode(str2.lower()))*1./max(len(unicode(value[1])),len(unicode(str2))))
    				d = min(result)
    	elif(isinstance(str2, list)):
    		for value in enumerate(str2):
    			result.append(ld(unicode(value[1].lower()), unicode(str1.lower()))*1./max(len(unicode(value[1])),len(unicode(str1))))
    		d = min(result);
    	else:
    		d = ld(unicode(str1), unicode(str2))*1./max(len(unicode(str1)),len(unicode(str2)))
    	return(d)
    
udf_leven_list = F.udf(leven_list, FloatType())
        
    



@app.route('/hello',methods =['POST'])
def classification():
	#grabs the Linkedin data tagged as 'linkedin'
	linkedin = request.get_json()['linkedin'] # dictionary object
        linkedin = json.dumps(linkedin) # convert the list object into a json-format string 
	github = request.get_json()['github']
	github = json.dumps(github)

	sc = init_spark_context()
        #sc = spark.sparkContext
	sqlContext = SQLContext(sc)


	#test= [{"interests": [], "linkedin": 213964086, "headline": "Software Development Engineer II at Amazon", "websites": [], "industry": "Information Technology and Services", "linkedin_url": "http://www.linkedin.com/in/cberkhoff"},{"interests": [], "linkedin": 213964086, "headline": "Software Development Engineer II at Amazon", "websites": [], "industry": "Information Technology and Services", "linkedin_url": "http://www.linkedin.com/in/cberkhoff"}]
        #test = json.dumps(test)
        rddLink = sc.parallelize([linkedin])
	rddGit = sc.parallelize([github])
        logger.info("rddLink loaded!")
        logger.info(linkedin[0:7000])
        logger.info(rddLink)
        logger.info(sc._conf.getAll())

	#link  = rddLink.map(lambda x: Row(**f(x))).toDF()
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

	data_join = git.select('id','git_lang','_skills', git.bio.alias('_edu_exp'), 'bio', git._pro_pub_desc.alias('exp_desc'), 'summary','_pro_pub_title','_pro_pub_desc').union(link.select('id','git_lang','_skills','_edu_exp','bio', 'exp_desc', 'summary','_pro_pub_title','_pro_pub_desc'))
	data_join = data_join.na.fill('')

	data_feature = model_TFIDF.transform(data_join)

	data_feature = data_feature.select('id', 'git_langIDF','_skillsIDF', '_edu_expIDF','bioIDF', 'exp_descIDF','summaryIDF','_pro_pub_titleIDF','_pro_pub_descIDF')
	
	joined = joined.withColumn('idLink', tostring_udf(joined.idLink))
	joined = joined.withColumn('idGit', uniqueId_udf(joined.idGit))

	lookupTable = sc.broadcast(data_feature.rdd.map(lambda x: (x['id'], 
                                             {'git_langIDF':x['git_langIDF'],
                                              '_skillsIDF':x['_skillsIDF'],
                                                   '_edu_expIDF':x['_edu_expIDF'],
                                               'bioIDF':x['bioIDF'],
                                                   'exp_descIDF':x['exp_descIDF'],
                                                   'summaryIDF':x['summaryIDF'],
                                                   '_pro_pub_titleIDF':x['_pro_pub_titleIDF'],
                                                   '_pro_pub_descIDF':x['_pro_pub_descIDF']})).collectAsMap())
	pairId = joined.select('idLink','idGit')
	pairPersonDF = pairId.rdd.map(lambda x: x + similarities(x[0], x[1], lookupTable))
	measureMapping = sqlContext.createDataFrame(pairPersonDF.map(lambda x: Row(idLink=x[0], 
                                                                          idGit=x[1], 
                                                                          gitlang_simi=float(x[2]),
                                                                          skill_simi = float(x[3]),
                                                                          edu_exp_simi = float(x[4]),
                                                                          bio_simi = float(x[5]),
                                                                          exp_desc_simi = float(x[6]),
                                                                          summary_simi = float(x[7]),
                                                                          pro_pub_title_simi = float(x[8]),
                                                                          pro_pub_desc_simi= float(x[9]))))
	joined_measure = joined.join(measureMapping, ['idGit', 'idLink'])
	joined_canonicalized = joined_measure
	for col_name in ['git_name','full_name']:
		joined_canonicalized = joined_canonicalized.withColumn(col_name, udf_preprocess(F.col(col_name)))
	joined_lower = joined_canonicalized
	for col_name in ['git_name','full_name']:
		joined_lower = joined_lower.withColumn(col_name, F.lower(F.col(col_name)))
    

	joined_lower = joined_lower.withColumn('git_company',udf_pre_company(F.col('git_company')))
	joined_lower = joined_lower.withColumn("name_leven", F.levenshtein(joined_lower['git_name'], joined_lower['full_name'])).withColumn("name_dmetaphone", udf_dmetaphone(joined_lower['git_name'],joined_lower['full_name'])).withColumn("name_jw", udf_jaro_winkler(joined_lower['git_name'],joined_lower['full_name'])).withColumn("name_fuzz",F.when(F.col('git_name').isNotNull(),udf_fuzz_sort(joined_lower['git_name'],joined_lower['full_name'])).otherwise(F.lit(None))).withColumn("login_fuzz", udf_fuzz_sort(joined_lower['git_login'], joined_lower['full_name'])).withColumn("login_jw",udf_jaro_winkler(joined_lower['git_login'], joined_lower['full_name']))
	joined_lower = joined_lower.withColumn("location_fuzz_sort", F.when(F.col('git_location').isNotNull(),udf_fuzz_sort(joined_lower['git_location'], joined_lower['location'])))
	joined_lower = joined_lower.withColumn("school_company", F.when(F.col('git_company').isNotNull(),udf_fuzz_sort(joined_lower['education.name'], joined_lower['git_company'])).otherwise(F.lit(None)))
	joined_lower = joined_lower.withColumn("company_company", F.when(F.col('git_company').isNotNull(),udf_fuzz_sort(joined_lower['experience.organization'], joined_lower['git_company'])).otherwise(F.lit(None)))
	joined_lower = joined_lower.withColumn("org_company", udf_fuzz_sort(joined_lower['experience.organization'], joined_lower['git_org']))
	joined_lower = joined_lower.withColumn("org_school", udf_fuzz_sort(joined_lower['education.name'], joined_lower['git_org']))
	joined_lower = joined_lower.withColumn("linkedin_gitblog", udf_leven_list('linkedin_url','git_blog'))
	joined_lower = joined_lower.withColumn("linkedin_gitweb", udf_leven_list('linkedin_url','git_websiteUrl'))
	joined_lower = joined_lower.withColumn("linkweb_gitblog", udf_leven_list('websites.url','git_blog'))
	joined_lower = joined_lower.withColumn("linkweb_gitweb", udf_leven_list('websites.url','git_websiteUrl'))
	joined_lower = joined_lower.withColumn("linkweb_github", udf_leven_list('websites.url','github_url'))
	count = joined_lower.cube("idLink").count()
	joined_lower= joined_lower.join(count,['idLink']) 
	feature = joined_lower.select('idGit','idLink','match','bio_simi','edu_exp_simi','gitlang_simi','pro_pub_title_simi','pro_pub_desc_simi','skill_simi','summary_simi','exp_desc_simi','count','name_leven','name_dmetaphone','name_jw','name_fuzz','login_fuzz','login_jw','location_fuzz_sort','school_company','company_company','linkedin_gitblog','linkedin_gitweb','linkweb_gitblog','linkweb_gitweb','linkweb_github')

	fill_values = {'bio_simi': -0.7676835240063782,'company_company': 13.027269554813302,'count': 69.97936515849672,'edu_exp_simi': -0.7674423412298906,'exp_desc_simi': -0.4794768606439826,'gitlang_simi': -0.36508407687661887,'linkedin_gitblog': 0.6704111164342388,'linkedin_gitweb': 0.6704111164342388,'linkweb_gitblog': 0.5065308678618619,'linkweb_github': 0.584223292437445,'linkweb_gitweb': 0.5065308678618619,'location_fuzz_sort': 34.35918477770764,'login_fuzz': 49.54616025581677,'login_jw': 0.638835025212626,'name_dmetaphone': 0.7347467665413555,'name_fuzz': 76.757491115495,'name_jw': 0.8106706649850334,'name_leven': 4.463146574747353,'pro_pub_desc_simi': -0.7593587783269232,'pro_pub_title_simi': -0.901583569164877,'school_company': 8.18176379679944, 'skill_simi': -0.29417421426755436, 'summary_simi': -0.9685668356171937}
	feature_f = feature.na.fill(fill_values).withColumnRenamed('match','label')

	assembler = VectorAssembler(inputCols=feature_f.columns[3:], outputCol="features")
	feature_assem = assembler.transform(feature_f)

	testRF= RandomForestClassificationModel.load("wasb://test-container@tfsmodelstorage.blob.core.windows.net/rfModel0914")

	rfPredict = testRF.transform(feature_assem)
	rfPredict = rfPredict.withColumn("label", rfPredict.label.cast("double"))

	positive = joined_measure.join(rfPredict.filter('prediction == 1.0'), ['idGit', 'idLink'])

	#positive = positive.toPandas()
        logger.info(positive.show())





      






	test = link.count()


        return ("hello, "+ linkedin[0]+"\n"+"test "+str(test)+"\n")


if __name__ == "__main__":
	print(("* Loading models and Flask starting server..."
		"please wait until server has fully started"))
	app.run(host='0.0.0.0', port=5002)

