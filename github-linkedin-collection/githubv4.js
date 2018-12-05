var express = require('express');
var unirest = require ('unirest');
const utility = require ('./utility');

const endpoint="https://api.github.com/graphql"
module.exports=function()
{

  var functions={}
    functions.user=function(req, res, login, action)
  {

    const token=req.body.credential.github_token;
    const proxy = req.body.credential.proxy;
    console.log("proxy:", proxy);
    var query={
      "query":'query($login:String!) { \
          user(login:$login) { \
          id \
          bio \
          html_url:url \
          repositoriesContributedTo(last:100){ \
            nodes{ \
              name \
              url \
              id \
            } \
          } \
          gists(first:100) \
          { \
            nodes{ \
              name \
            } \
          } \
          company \
          blog:websiteUrl \
          name \
          followers{ \
            totalCount \
          } \
          following{ \
            totalCount \
          } \
          organizations(first: 100) { \
            edges { \
              node { \
                id \
                name \
              } \
            } \
          } \
          avatarUrl \
          email \
          location \
          login \
          websiteUrl \
          repositories(last:100, orderBy: {field: UPDATED_AT, direction: DESC}) { \
          nodes{ \
            id\
            createdAt \
            description \
            name \
            isFork \
            url \
            stargazers \
            { \
              totalCount \
            } \
            watchers \
            { \
              totalCount \
            } \
            primaryLanguage { \
              name \
            } \
            resourcePath \
            readme: object(expression: "master:README.md") { \
                 ... on Blob { \
               text \
            } \
              } \
               ref(qualifiedName: "master") { \
            target { \
              ... on Commit { \
                history(first: 20) { \
                  edges { \
                    node { \
                      author { \
                        email \
                        name \
                        user { \
                          login \
                        } \
                      } \
                    } \
                  } \
                } \
              } \
            } \
          } \
            repositoryTopics(first:100){ \
              edges{ \
                node{ \
                  topic { \
                    name \
                  } \
                } \
              } \
            } \
            languages (first:100) { \
              edges { \
                node { \
                  name \
                } \
              } \
            } \
          } \
          } \
        } \
      }',
      "variables":null

    };

    var varables={"login":login}; 
    query.variables=JSON.stringify(varables) // converting a JS object into a string following the JSON notation

    unirest.post(endpoint)
    .headers({"Content-Type": "text/html" ,"Accept": 'application/json' ,"Authorization": "bearer "+token, 'user-agent': 'node.js'})
    .proxy(proxy)
    .send(JSON.stringify(query)) //creating requests
    .end(function(response){
      // send HTTP requests and awaits Response finalization. Upon HTTP Response post-processing occurs and invokes callback with a single argument the response object 

      if(!response || response.statusCode==401 || !response.headers ||response.headers["x-ratelimit-limit"]==0)
      {
        console.log("entered a invalid github API token... trying to replace.")
        utility.replaceInvalidGithubToken(req.body.credential.github_token, (response && response.statusCode==401), function(newSet, e){
          if(e)
          {
            console.log(e);
            res.status(300).json({"error":"internal db error", "data":null, "headers":response.headers});
            return;
          }
          else {


            req.body.credential.github_token=newSet.token;
            req.body.credential.proxy=newSet.proxy;
            functions.user(req, res, login, action);
            return;
          }
        });
      }
      else {
          res.status(response.statusCode).json({"error":response.body?response.body.message:null, "data":response.body?response.body.data:null, "headers":response.headers});
          return;
      }



      return;
    });
  }

// Search similar github profiles based on names
  functions.search=function(req, res, name, after, action)
  {

    const token=req.body.credential.github_token;
    const proxy = req.body.credential.proxy;
    console.log("proxy:", proxy);
    var search_query={
      "search_query":'query($name:String!, $after: String!) { \
          search(query: $name, type : USER, first : 2, after : $after) { \
            nodes{\
              ...on User{\
                     id \
                    bio \
                    html_url:url \
                    repositoriesContributedTo(last:100){ \
                      nodes{ \
                        name \
                        url \
                        id \
                      } \
                    } \
                   gists(first:100){ \
                     nodes{ \
                      name \
                    } \
                  } \
                 company \
                 blog:websiteUrl \
                 name \
                 followers{ \
                   totalCount \
                 } \
                 following{ \
                   totalCount \
                } \
                 organizations(first: 100) { \
                   edges { \
                    node { \
                      id \
                      name \
                       } \
                     } \
                    } \
                 avatarUrl \
                 email \
                 location \
                 login \
                 websiteUrl \
                repositories(last:100, orderBy: {field: UPDATED_AT, direction: DESC}) { \
                   nodes{ \
                     id\
                     createdAt \
                     description \
                     name \
                     isFork \
                     url \
                     stargazers \
                       { \
                        totalCount \
                        } \
                     watchers \
                       { \
                         totalCount \
                        } \
                      primaryLanguage { \
                         name \
                        } \
                      resourcePath \
                        readme: object(expression: "master:README.md") { \
                         ... on Blob { \
                         text \
                          } \
                         } \
                      ref(qualifiedName: "master") { \
                     target { \
                       ... on Commit { \
                          history(first: 20) { \
                            edges { \
                               node { \
                                author { \
                                  email \
                                  name \
                                  user { \
                                  login \
                              } \
                            } \
                           } \
                          } \
                        } \
                      } \
                    } \
                  } \
               repositoryTopics(first:100){ \
                  edges{ \
                     node{ \
                       topic { \
                         name \
                        } \
                      } \
                    } \
                  } \
                languages (first:100) { \
                     edges { \
                        node { \
                          name \
                          } \
                        } \
                      } \
                    } \
                  } \
                } \
               } \
              userCount \
              pageInfo{ endCursor}\
            }\
          }\
      }',
      "variables":null

    };

    var search_variables={"name":name, "after": after }; 
    search_query.variables=JSON.stringify(variables) // converting a JS object into a string following the JSON notation

    unirest.post(endpoint)
    .headers({"Content-Type": "text/html" , "Authorization": "bearer "+token, 'user-agent': 'node.js'})
    .proxy(proxy)
    .send(JSON.stringify(search_query)) //creating requests
    .end(function(response, request){

      if(!response || response.statusCode==401 || !response.headers ||response.headers["x-ratelimit-limit"]==0)
      {
        console.log("entered a invalid github API token... trying to replace.")
        utility.replaceInvalidGithubToken(req.body.credential.github_token, (response && response.statusCode==401), function(newSet, e){
          if(e)
          {
            console.log(e);
            res.status(300).json({"error":"internal db error", "data":null, "headers":response.headers});
            return;
          }
          else {


            req.body.credential.github_token=newSet.token;
            req.body.credential.proxy=newSet.proxy;
            functions.search(req, res, search, action);
            return;
          }
        });
      }
      else {
          res.status(response.statusCode).json({"error":response.body?response.body.message:null, "data":response.body?response.body.data:null, "headers":response.headers});
          return;
      }



      return;
    });
  }

  functions.process = function (req, res) {
      switch(req.params.sub) // an object containing properties mapped to the named route"parameters"
      {
        case "user":
          functions.user(req, res, req.params.subsub, req.params.subsubsuburl);
          break;
        case "search":
          functions.search(req, res, req.params.subsub, req.params.subsubsuburl);
          break;          
        default:
          res.status(400).json({"error":"no resource specificed."})
          break;
      }
    }


    return functions;
}

