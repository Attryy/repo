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


class FeatureExtraction:
    '''A feature extraction module
    '''
    @staticmethod
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

    @staticmethod
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

    @staticmethod
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


    @staticmethod
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
                      d = max(result);
                  else:
                      for value in enumerate(str1):
                          result.append(fuzz.token_sort_ratio(preprocess_company(value[1]), preprocess_company(str2)))
                      d = max(result)   
              elif(isinstance(str2, list)):
                  for value in enumerate(str2):
                      result.append(fuzz.token_sort_ratio(preprocess_company(value[1]), preprocess_company(str1)))
                  d = max(result);
              else:
                  d = fuzz.token_sort_ratio(str1, str2)
              return(d)


    @staticmethod
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




    def __init__(self, sc,sqlContext, LinkedinData, GithubData):
	'''initialize the feature extraction module given a spark context, a linkedin dataframe and a github dataframe
	'''

   	logger.info('Starting the feature extraction process...')

	self.sc = sc
	self.sqlContext = sqlContext

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

	self.LinkedinData = LinkedinData

	logger.info('Cleaning the formatting for Github data...')

	GithubData = GithubData.withColumn('git_org', F.concat_ws(',',F.col('git_org')))
	GithubData = GithubData.withColumn('_pro_pub_title', F.concat_ws(',', F.col('repos.name')))
	GithubData = GithubData.withColumn('_pro_pub_desc', F.concat_ws(',', F.col('repos.description')))
	GithubData = GithubData.withColumn('git_lang', F.col('repos.lang').cast("string"))
	GithubData = GithubData.withColumn('_skills', F.concat_ws(',', F.col('git_lang'),F.col('_pro_pub_title')))
	GithubData = GithubData.withColumn('summary', F.concat_ws(',', F.col('_pro_pub_title'),F.col('git_email'), F.col('bio'), F.col('git_company')))
	GithubData = GithubData.withColumn('id', F.col('gid').cast("string"))
	GithubData = GithubData.withColumn('id', F.regexp_replace(F.col('id'), '([\d]+)', "g$1"))

        self.GithubData = GithubData

	self.model_TFIDF = PipelineModel.load("wasb://test-container@tfsmodelstorage.blob.core.windows.net/model_tfidf0913")

	data_join = self.GithubData.select('id','git_lang','_skills', self.GithubData.bio.alias('_edu_exp'), 'bio', self.GithubData._pro_pub_desc.alias('exp_desc'), 'summary','_pro_pub_title','_pro_pub_desc').union(self.LinkedinData.select('id',self.LinkedinData.skills.alias('git_lang'),'_skills','_edu_exp',self.LinkedinData.headline.alias('bio'), 'exp_desc', 'summary','_pro_pub_title','_pro_pub_desc'))
        self.data_join = data_join.na.fill('')

        self.data_feature = self.model_TFIDF.transform(self.data_join).select('id', 'git_langIDF','_skillsIDF', '_edu_expIDF','bioIDF', 'exp_descIDF','summaryIDF','_pro_pub_titleIDF','_pro_pub_descIDF')
        self.lookupTable = self.sc.broadcast(self.data_feature.rdd.map(lambda x: (x['id'], 
                                                  {'git_langIDF':x['git_langIDF'],
                                                   '_skillsIDF':x['_skillsIDF'],
                                                   '_edu_expIDF':x['_edu_expIDF'],
                                                   'bioIDF':x['bioIDF'],
                                                   'exp_descIDF':x['exp_descIDF'],
                                                   'summaryIDF':x['summaryIDF'],
                                                   '_pro_pub_titleIDF':x['_pro_pub_titleIDF'],
                                                   '_pro_pub_descIDF':x['_pro_pub_descIDF']})).collectAsMap())
        logger.info(self.lookupTable)
        joined = self.GithubData.select('gid', 'linkedin_id', 'git_login','git_name','git_location','git_company','git_blog','github_url','git_websiteUrl').join(self.LinkedinData.select('linkedin_id','full_name','location','edu_name','exp_org','linkedin_url',self.LinkedinData.websites.url.alias('website')), ['linkedin_id'])
        joined = joined.withColumn('gid', F.regexp_replace(F.col('gid'), '([\d]+)', 'g$1'))
        count = joined.cube("linkedin_id").count()
	joined = joined.join(count,['linkedin_id']) 
        self.joined = joined.withColumn('linkedin_id', F.col('linkedin_id').cast('string'))

        self.df = self.joined.toPandas()





