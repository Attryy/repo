
var async= require('async');
var unirest = require('unirest');
var async= require('async');
var sleep = require('sleep');
var ztable = require('ztable');


/*
sourcing settings

{"settings": {
    "analysis": {
      "filter": {
        "talentful_synonym": {
          "type": "synonym",
          "synonyms_path" : "synonyms.txt"
        }
      },
      "analyzer": {
        "talentful_synonym_analyzer": {
          "tokenizer": "whitespace",
          "filter": [
            "lowercase",
            "talentful_synonym"
          ]
        }
      }
    }
  },
   "mappings": {
      "profile": {
        "properties": {
          "id": {
            "type": "long"
          },
          "locations" : {"type" : "geo_point"},
          "companies": {
            "type": "long"
          },
          "website": {
            "type": "string",
            "analyzer":"talentful_synonym_analyzer"
          },
          "tweets": {
            "type": "string",
            "analyzer":"talentful_synonym_analyzer"
          },
          "github": {
            "properties": {
              "_type": {
                "type": "string"
              },
              "id": {
                "type": "long"
              },
              "bio": {
                    "type": "string",
                    "analyzer":"talentful_synonym_analyzer"
                  },
            "company": {
                    "type": "string"
                  },
              "repos": {
                "properties": {
                  "readme": {
                    "type": "string",
                    "analyzer":"talentful_synonym_analyzer"
                  },
                  "stargazers_count": {
                    "type": "long"
                  },
                  "fork": {
                    "type": "boolean"
                  },
                  "forks_count": {
                    "type": "long"
                  },
                  "watchers_count": {
                    "type": "long"
                  },
                  "language": {
                    "type": "string",
                    "analyzer":"talentful_synonym_analyzer"
                  },
                  "description": {
                    "type": "string",
                    "analyzer":"talentful_synonym_analyzer"
                  }
                }
              },
              "totalRepos":{
                "type": "long"
              },
              "totalFollowing": {
                "type": "long"
              },
              "totalFollowers": {
                "type": "long"
              },
              "totalGists": {
                "type": "long"
              },
              "repo_score":{
                "type": "long"
              }
            }
          },
          "meetup": {
            "properties": {
              "_type": {
                "type": "string"
              },
              "id": {
                "type": "long"
              },
              "totalTechGroups": {
                "type": "long"
              },
              "topics": {
                "properties": {
                  "personal": {
                    "type": "string",
                    "analyzer":"talentful_synonym_analyzer"
                  },
                  "groups": {
                    "type": "string",
                    "analyzer":"talentful_synonym_analyzer"
                  }
                }
              },
              "groups":{
                "type": "long"
              },
              "bio": {
                "type": "string",
                "analyzer":"talentful_synonym_analyzer"
              }
            }
          }
        }
      }
    }
    }
*/
/*
{
  "statusCode": 200,
  "body": {
    "profiles": {
      "aliases": {
      },
      "mappings": {
        "profile": {
          "properties": {
            "companies": {
              "type": "long"
            },
            "github": {
              "properties": {
                "_type": {
                  "type": "string"
                },
                "bio": {
                  "type": "string",
                  "analyzer": "talentful_synonym_analyzer"
                },
                "company": {
                  "type": "string"
                },
                "id": {
                  "type": "long"
                },
                "repo_score": {
                  "type": "long"
                },
                "repos": {
                  "properties": {
                    "description": {
                      "type": "string",
                      "analyzer": "talentful_synonym_analyzer"
                    },
                    "fork": {
                      "type": "boolean"
                    },
                    "forks_count": {
                      "type": "long"
                    },
                    "language": {
                      "type": "string",
                      "analyzer": "talentful_synonym_analyzer"
                    },
                    "readme": {
                      "type": "string",
                      "analyzer": "talentful_synonym_analyzer"
                    },
                    "stargazers_count": {
                      "type": "long"
                    },
                    "watchers_count": {
                      "type": "long"
                    }
                  }
                },
                "totalFollowers": {
                  "type": "long"
                },
                "totalFollowing": {
                  "type": "long"
                },
                "totalGists": {
                  "type": "long"
                },
                "totalRepos": {
                  "type": "long"
                }
              }
            },
            "id": {
              "type": "long"
            },
            "linkedin": {
              "properties": {
                "_type": {
                  "type": "string"
                },
                "edu": {
                  "properties": {
                    "begin": {
                      "type": "string"
                    },
                    "degree": {
                      "type": "string"
                    },
                    "end": {
                      "type": "string"
                    },
                    "school": {
                      "type": "string",
                      "analyzer": "talentful_synonym_analyzer"
                    }
                  }
                },
                "exp": {
                  "properties": {
                    "begin": {
                      "type": "string"
                    },
                    "company": {
                      "type": "string",
                      "analyzer": "talentful_synonym_analyzer"
                    },
                    "company_id": {
                      "type": "string"
                    },
                    "end": {
                      "type": "string"
                    },
                    "summary": {
                      "type": "string",
                      "analyzer": "talentful_synonym_analyzer"
                    },
                    "title": {
                      "type": "string",
                      "analyzer": "talentful_synonym_analyzer"
                    }
                  }
                },
                "id": {
                  "type": "long"
                },
                "location": {
                  "type": "string"
                },
                "name": {
                  "type": "string"
                },
                "public_url": {
                  "type": "string"
                },
                "skills": {
                  "type": "string",
                  "analyzer": "talentful_synonym_analyzer"
                },
                "summary": {
                  "type": "string"
                }
              }
            },
            "meetup": {
              "properties": {
                "_type": {
                  "type": "string"
                },
                "bio": {
                  "type": "string",
                  "analyzer": "talentful_synonym_analyzer"
                },
                "id": {
                  "type": "long"
                },
                "topics": {
                  "properties": {
                    "groups": {
                      "type": "string",
                      "analyzer": "talentful_synonym_analyzer"
                    },
                    "personal": {
                      "type": "string",
                      "analyzer": "talentful_synonym_analyzer"
                    }
                  }
                },
                "totalTechGroups": {
                  "type": "long"
                }
              }
            },
            "tweets": {
              "type": "string",
              "analyzer": "talentful_synonym_analyzer"
            },
            "website": {
              "type": "string",
              "analyzer": "talentful_synonym_analyzer"
            }
          }
        }
      },
      "settings": {
        "index": {
          "creation_date": "1485326094048",
          "uuid": "OJCoSKmaTaeYfS0k30Fa8g",
          "analysis": {
            "filter": {
              "talentful_synonym": {
                "type": "synonym",
                "synonyms_path": "synonyms.txt"
              }
            },
            "analyzer": {
              "talentful_synonym_analyzer": {
                "filter": [
                  "lowercase",
                  "talentful_synonym"
                ],
                "tokenizer": "whitespace"
              }
            }
          },
          "number_of_replicas": "1",
          "number_of_shards": "5",
          "version": {
            "created": "2040199"
          }
        }
      },
      "warmers": {
      }
    }
  },
  "headers": {
    "content-type": "application/json; charset=UTF-8",
    "content-length": "2373"
  },
  "request": {
    "uri": {
      "protocol": "http:",
      "slashes": true,
      "auth": null,
      "host": "localhost:9200",
      "port": "9200",
      "hostname": "localhost",
      "


      ": null,
      "search": null,
      "query": null,
      "pathname": "/profiles",
      "path": "/profiles",
      "href": "http://localhost:9200/profiles"
    },
    "method": "get",
    "headers": {
      "content-length": 0
    }
  }
}
*/
/*
{"mappings": {
      "profile": {
        "properties": {
          "id": {
            "type": "long"
          },
          "locations" : {"type" : "geo_point"},
          "company": {
            "type": "long"
          },
          "website": {
            "type": "string",
            "analyzer":"talentful_synonym_analyzer"
          },
          "tweets": {
            "type": "string",
            "analyzer":"talentful_synonym_analyzer"
          },
          "github": {
            "properties": {
              "_type": {
                "type": "string"
              },
              "id": {
                "type": "long"
              },
              "bio": {
                    "type": "string",
                    "analyzer":"talentful_synonym_analyzer"
                  },
            "company": {
                    "type": "string"
                  },
              "repos": {
                "properties": {
                  "readme": {
                    "type": "string",
                    "analyzer":"talentful_synonym_analyzer"
                  },
                  "stargazers_count": {
                    "type": "long"
                  },
                  "fork": {
                    "type": "boolean"
                  },
                  "forks_count": {
                    "type": "long"
                  },
                  "watchers_count": {
                    "type": "long"
                  },
                  "language": {
                    "type": "string",
                    "analyzer":"talentful_synonym_analyzer"
                  },
                  "description": {
                    "type": "string",
                    "analyzer":"talentful_synonym_analyzer"
                  }
                }
              },
              "totalRepos":{
                "type": "long"
              },
              "totalFollowing": {
                "type": "long"
              },
              "totalFollowers": {
                "type": "long"
              },
              "totalGists": {
                "type": "long"
              },
              "repo_score":{
                "type": "long"
              }
            }
          },
          "meetup": {
            "properties": {
              "_type": {
                "type": "string"
              },
              "id": {
                "type": "long"
              },
              "totalTechGroups": {
                "type": "long"
              },
              "topics": {
                "properties": {
                  "personal": {
                    "type": "string",
                    "analyzer":"talentful_synonym_analyzer"
                  },
                  "groups": {
                    "type": "string",
                    "analyzer":"talentful_synonym_analyzer"
                  }
                }
              },
              "bio": {
                "type": "string",
                "analyzer":"talentful_synonym_analyzer"
              }
            }
          }
        }
      }
    }
}
*/


Array.prototype.inArray = function(value) {
    for(var i=0; i < this.length; i++) {
        if(value==this[i]) return true;
    }
    return false;
};



module.exports=function(res)
{
  var functions={};
  var constants = require('./constants');
  var es_urls=constants.es_urls;


  

  functions.bulkUploadRepos=function(repos, callback)
  {
    /*
    {
      "upsert":[
          { "update" : {"_id" : 49459226, "_type" : "repo", "_index" : "suggester"} },
          { "doc" : {"id": 49459226, "urlname": "https://github.com/cyl3392207/msblogs", "name": "msblogs", "description": "myblogs", "lang": "powershell"}, "doc_as_upsert" : true }
        ]
    }*/
    var json={
      "upsert":[]
    }
    if(!repos || repos.length==0)
    {
      callback();
      return
    }
    for(var i=0;i<repos.length;i++)
    {
      const repo=repos[i];
      json.upsert.push({ "update" : {"_id" : repo.id, "_type" : "repo", "_index" : "suggester"} });
      json.upsert.push({ "doc" : {"id": repo.id, "urlname": repo.url, "name": repo.name, "description": repo.description, "lang": repo.lang, "readme":repo.readme}, "doc_as_upsert" : true });
    }
    var url=es_urls.base+'_bulk?secret=xLoSl0N2YJ6evdDbFGjJ';
    console.log(url,"\n");
    unirest.post(url)
          .headers({'content-type': 'application/json'})
          .send(json)
          .end(function (response) {
            if(response.body && response.body.statusCode!=201 && response.body.statusCode!=200 && response.body.statusCode!=409)
            {
              console.log("error: ", JSON.stringify(response), "\n -->", json);
              throw "error";
              return;
            }
            else {
              console.log("successfully uploaded...");
             // console.log("successfully uploaded....", response.body);
              callback();
              return;

            }

          });

  }
  
    return functions;
}
