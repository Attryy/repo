#! /usr/bin/env python3
'''
This file launches a Spark session and a flask server to predict the potential Github matches given linkedin and github data set that are linkedin through linkedin_id.
To execute the file, download it into the $SPARK_HOME directory on the virtual machine that is the master of the cluster first.
Then run sudo ./bin/spark-submit predict_scikitLearn.py
The linkedin and github data can be send to the server by POST request and results of the format {"git_login":XX, "probability": XX} would be returned. 
'''
import warnings
warnings.filterwarnings('ignore')
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
from jellyfish import levenshtein_distance as ld
import pickle

from featureExtraction import FeatureExtraction 

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




#initialize the server 
app = flask.Flask(__name__)

#define a /hello route for only post requests
def init_spark_context():
    conf = SparkConf().setAppName("github-linkedin-server").setMaster("spark://10.8.0.14:7077").set("spark.driver.maxResultSize", "3.5g").set('spark.cleaner.referenceTracking','false').set("spark.executor.extraClassPath", "/opt/spark/spark-2.3.1-bin-hadoop2.7/jars/*.jar").set("spark.memory.offHeap.enabled","true").set("spark.memory.offHeap.size","5g").set("spark.jars","file:////opt/spark/spark-2.3.1-bin-hadoop2.7/jars/azure-storage-2.0.0.jar,file:////opt/spark/spark-2.3.1-bin-hadoop2.7/jars/hadoop-azure-2.7.7.jar").set("spark.network.timeout", "360s").set("spark.executor.heartbeatInterval","15s").set("spark.sql.catalogImplementation","hive").set("spark.dynamicAllocation.enabled","false").set("spark.driver.memory","12g").set("spark.executor.memory","26g").set("spark.driver.maxResultSize","4g").set("spark.executor.cores","3").set("spark.jars","file:////opt/spark/spark-2.3.1-bin-hadoop2.7/jars/azure-storage-2.0.0.jar,file:////opt/spark/spark-2.3.1-bin-hadoop2.7/jars/hadoop-azure-2.7.7.jar").set("spark.submit.deployMode", "client").set("spark.driver.port","40015").set("spark.blockManager.port","40045").set("spark.driver.blockManager.port","40075").set("spark.worker.port", "40105").set("spark.shuffle.service.port", "40135")
   # spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    #sc = spark.sparkContext
    sc = SparkContext(conf=conf) #, pyFiles=['featureExtraction.py'])
    sc._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb")
    sc._jsc.hadoopConfiguration().set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.azure.account.key.tfsmodelstorage.blob.core.windows.net", "WOrz8vS02QqLPDn5vWm6HQzpxInSxJ/yJP5eyhKVRTZ1tK86oJIYxSmK/xzUJHc+vfHhNFM5fH8kqNcr8C+bww==")
    #sc = spark.sparkContext
    return sc



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
        from name_tools import split
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


def similarities(linkedin_id, gid, lookupTable):
          X, Y = lookupTable[linkedin_id], lookupTable[gid]

          def _cosine_similarity(X, Y):
              denom = np.sqrt(X.multiply(X).sum(1)).item() *np.sqrt(Y.multiply(Y).sum(1)).item() 
              if denom == 0.0:
                  return -1.0
              else:
                 return X.multiply(Y).sum()*1. / float(denom)

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

def warn(*args, **kwargs):
    pass


@app.route('/predict',methods =['POST','GET'])
def classification():

        import warnings
        warnings.warn = warn

	#grabs the Linkedin data tagged as 'linkedin' as a dictionary object 
	linkedin = request.get_json()['linkedin']
        # convert the dictionary object into a json-format string 
        linkedin = json.dumps(linkedin) 

	github = request.get_json()['github']
	github = json.dumps(github)

        #specify the github and linkedin data schema to avoid empty field not being included
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


        # initialize the spark session and sqlContext 
	sc = init_spark_context()
	sqlContext = SQLContext(sc)

        rddLink = sc.parallelize([linkedin])
	rddGit = sc.parallelize([github])

        LinkedinData = sqlContext.read.json(rddLink, LinkSchema)
        GithubData = sqlContext.read.json(rddGit, GitSchema)

	LinkedinData.persist()
	GithubData.persist()

        logger.info('Linkedin data loaded!')
        logger.info('Github data loaded!')

        logger.info('Cleaning the formatting for Linkedin data...')

        LinkedinData= LinkedinData.withColumnRenamed('linkedin', 'linkedin_id')#.drop('linkedin_id').withColumnRenamed('linkedin','linkedin_id')
        LinkedinData = LinkedinData.withColumn('skills', F.concat_ws(',',LinkedinData.skills))
        LinkedinData= LinkedinData.withColumn('edu_name', F.concat_ws(',', F.col('education.name')))
        LinkedinData = LinkedinData.withColumn('exp_org', F.concat_ws(',', F.col('experience.organization')))
        LinkedinData = LinkedinData.withColumn('_pro_pub_title', F.concat_ws(',', F.col('projects.title'), F.col('publications.name')))
        LinkedinData = LinkedinData.withColumn('_pro_pub_desc', F.concat_ws(',', F.col('projects.description'), F.col('publications.summary')))
        LinkedinData = LinkedinData.withColumn("exp_desc", F.concat_ws(',', F.col('experience.description')))
        LinkedinData = LinkedinData.withColumn('_skills', F.concat_ws(',', F.col('skills'), F.col('projects.title'), F.col('publications.name')))
        LinkedinData = LinkedinData.withColumn('_edu_exp', F.concat_ws(',', F.col('education.name'), F.col('experience.organization')))
        LinkedinData = LinkedinData.withColumn('id', F.col('linkedin_id').cast('string'))



        logger.info('Cleaning the formatting for Github data...')

        GithubData = GithubData.withColumn('git_org', F.concat_ws(',',F.col('git_org')))
        GithubData = GithubData.withColumn('_pro_pub_title', F.concat_ws(',', F.col('repos.name')))
        GithubData = GithubData.withColumn('_pro_pub_desc', F.concat_ws(',', F.col('repos.description')))
        GithubData = GithubData.withColumn('git_lang', F.col('repos.lang').cast("string"))
        GithubData = GithubData.withColumn('_skills', F.concat_ws(',', F.col('git_lang'),F.col('_pro_pub_title')))
        GithubData = GithubData.withColumn('summary', F.concat_ws(',', F.col('_pro_pub_title'),F.col('git_email'), F.col('bio'), F.col('git_company')))
        GithubData = GithubData.withColumn('id', F.col('gid').cast("string"))
        GithubData = GithubData.withColumn('id', F.regexp_replace(F.col('id'), '([\d]+)', "g$1"))



        data_join = GithubData.select('id','git_lang','_skills', GithubData.bio.alias('_edu_exp'), 'bio', GithubData._pro_pub_desc.alias('exp_desc'), 'summary','_pro_pub_title','_pro_pub_desc').union(LinkedinData.select('id',LinkedinData.skills.alias('git_lang'),'_skills','_edu_exp',LinkedinData.headline.alias('bio'), 'exp_desc', 'summary','_pro_pub_title','_pro_pub_desc'))
        data_join = data_join.na.fill('')

        data_joinDF = data_join.toPandas()

        for name in ['git_lang','_skills','_edu_exp','bio',
             'exp_desc','summary','_pro_pub_title','_pro_pub_desc']:
            model_path = '/home/jia/Dropbox/Startup/code/Talentful/data/'+name+'.pk'
            loaded_vectorizer = pickle.load(open(model_path, 'rb'))
            logger.info('Loading TF-IDF model for '+ name+'...')
            vector = list(loaded_vectorizer.transform(data_joinDF.loc[:, name]))
            data_joinDF.loc[:,name+'IDF'] = vector
        
        logger.info('Creating lookupTable...')

        lookupTable = data_joinDF.loc[:, ['id','git_langIDF','_skillsIDF','_edu_expIDF','bioIDF', 'exp_descIDF','summaryIDF','_pro_pub_titleIDF','_pro_pub_descIDF']].set_index('id').transpose().to_dict(orient = 'dict')

        joined = GithubData.select('gid', 'linkedin_id', 'git_login','git_name','git_location','git_company','git_blog','github_url','git_websiteUrl').join(LinkedinData.select('linkedin_id','full_name','location','edu_name','exp_org','linkedin_url',LinkedinData.websites.url.alias('website')), ['linkedin_id'])
        joined = joined.dropDuplicates(['gid','linkedin_id'])
        joined = joined.withColumn('gid', F.regexp_replace(F.col('gid'), '([\d]+)', 'g$1'))
        count = joined.cube("linkedin_id").count()
        joined = joined.join(count,['linkedin_id'])
        joined = joined.withColumn('linkedin_id', F.col('linkedin_id').cast('string'))

	pairId = joined.select('linkedin_id','gid')
	pairPersonDF = pairId.rdd.map(lambda x: x + similarities(x[0], x[1], lookupTable))
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
	
	df = joined.toPandas()
        df['name_leven'] = df.apply(lambda row: ld(row['git_name'], row['full_name']), axis = 1)
        df['name_dmetaphone'] = df.apply(lambda row: doublemetaphone(row['git_name'], row['full_name']), axis = 1)
        df['name_jw'] = df.apply(lambda row: jaro_winkler(row['git_name'], row['full_name']), axis = 1)
        df['name_fuzz'] = df.apply(lambda row: fuzz_sort(row['git_name'], row['full_name']), axis = 1)

	df['git_company'] = df.apply(lambda row: preprocess_company(row['git_company']), axis = 1)
	for column in ['git_name','full_name']:
           df[column] = df.apply(lambda row: preprocess(row[column]), axis = 1)

        df['login_fuzz'] = df.apply(lambda row: fuzz_sort(row['git_login'], row['full_name']), axis = 1)
        df['login_jw'] = df.apply(lambda row: jaro_winkler(row['git_login'], row['full_name']), axis = 1)
        df['location_fuzz_sort'] = df.apply(lambda row: fuzz_sort(row['git_location'], row['location']), axis = 1)
        df['school_company'] = df.apply(lambda row: fuzz_sort(row['git_company'], row['edu_name']), axis = 1)
        df['company_company'] = df.apply(lambda row: fuzz_sort(row['git_company'], row['exp_org']), axis = 1)
        df['linkedin_gitblog'] = df.apply(lambda row: leven_list(row['linkedin_url'], row['git_blog']), axis = 1)
        df['linkedin_gitweb'] = df.apply(lambda row: leven_list(row['linkedin_url'], row['git_websiteUrl']), axis = 1)
        df['linkweb_gitblog'] = df.apply(lambda row: leven_list(row['website'], row['git_blog']), axis = 1)
        df['linkweb_gitweb'] = df.apply(lambda row: leven_list(row['website'], row['git_websiteUrl']), axis = 1)
        df['linkweb_github'] = df.apply(lambda row: leven_list(row['website'], row['github_url']), axis = 1)


        # specify the schema of the feature dataframe
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

	fill_values = {'bio_simi': -0.7676835240063782,'company_company': 13.027269554813302,'count': 69.97936515849672,'edu_exp_simi': -0.7674423412298906,'exp_desc_simi': -0.4794768606439826,'gitlang_simi': -0.36508407687661887,'linkedin_gitblog': 0.6704111164342388,'linkedin_gitweb': 0.6704111164342388,'linkweb_gitblog': 0.5065308678618619,'linkweb_github': 0.584223292437445,'linkweb_gitweb': 0.5065308678618619,'location_fuzz_sort': 34.35918477770764,'login_fuzz': 49.54616025581677,'login_jw': 0.638835025212626,'name_dmetaphone': 0.7347467665413555,'name_fuzz': 76.757491115495,'name_jw': 0.8106706649850334,'name_leven': 4.463146574747353,'pro_pub_desc_simi': -0.7593587783269232,'pro_pub_title_simi': -0.901583569164877,'school_company': 8.18176379679944, 'skill_simi': -0.29417421426755436, 'summary_simi': -0.9685668356171937}

        logger.info('Imputing the NA values in the features...')
        feature_f = feature.na.fill(fill_values)
        feature_f.persist()
        featureDF = feature_f.toPandas()

	filename = '/home/jia/Dropbox/Startup/code/Talentful/data/rfModel.sav'
        loaded_model = pickle.load(open(filename, 'rb'))
        logger.info('Classification Model loaded!')
        logger.info('Predicting the potential match...')

        y_prob = loaded_model.predict_proba(featureDF[['bio_simi','edu_exp_simi','gitlang_simi','pro_pub_title_simi','pro_pub_desc_simi','skill_simi','summary_simi','exp_desc_simi','count','name_leven','name_dmetaphone','name_jw','name_fuzz','login_fuzz','login_jw','location_fuzz_sort','school_company','company_company','linkedin_gitblog','linkedin_gitweb','linkweb_gitblog','linkweb_gitweb','linkweb_github']])

        y_score = loaded_model.predict(featureDF[['bio_simi','edu_exp_simi','gitlang_simi','pro_pub_title_simi','pro_pub_desc_simi','skill_simi','summary_simi','exp_desc_simi','count','name_leven','name_dmetaphone','name_jw','name_fuzz','login_fuzz','login_jw','location_fuzz_sort','school_company','company_company','linkedin_gitblog','linkedin_gitweb','linkweb_gitblog','linkweb_gitweb','linkweb_github']])
        featureDF['prediction'] = y_score
        featureDF['probability'] = [prob[1] for prob, score in zip(y_prob, y_score)]

        name_list = df[['linkedin_id','gid','git_login','full_name']]

        result = featureDF[featureDF['prediction'] == 1][['linkedin_id','gid','probability']].merge(name_list, on = ['linkedin_id','gid'])


        return (result[['git_login','probability']].to_json(orient = 'records'))



if __name__ == "__main__":
	print(("* Loading models and Flask starting server..."
		"please wait until server has fully started"))
	app.run(host='0.0.0.0', port=5004)

