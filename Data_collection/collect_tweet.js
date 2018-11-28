var async = require('async');
var sleep = require('sleep');


var functions = {};
var TwitterBot = require('./TwitterBot.js')
var db = require('./db_write.js');


functions.getTweets=function(){
    var sql_user = "select * from twitter_users where nonexist is false and (last_tweet_crawled is null or EXTRACT(epoch FROM age(now(), last_tweet_crawled))/86400 >90) order by id asc limit 1"
    var end_of_query=false;

    async.until(function(){return end_of_query},
    function(next_user){
    db.runAnyQuery(sql_user, null, function(e, r){
      if(e){
           console.log(e);
           throw e;
           return;
      }
     if(r.rows.length==0){
          console.log("No twitter account found within the time period");
          end_of_query=true;
          next_user();
          return;
      }

      var row=r.rows[0];
      console.log("\n=====new user=======:"+row.link);
      var max_id;
 
      var params = {screen_name: row.link, count: 200, trim_user:true};
      var end_of_query_sub=false;
      var total_tweet = 0;

     
      async.until(function(){return end_of_query_sub},
          function(next_timeline){
              console.log(total_tweet + " tweets collected..");

              if(total_tweet >=500){
                   end_of_query_sub = true;
                   next_timeline();
                   return;
              }
              if(max_id){
                   params['max_id']=max_id;
              }
              var client = new TwitterBot();
              client.getContent('statuses/user_timeline.json', params, function(error, json, response){
                     console.log('new timeline heard back ' + params.max_id);
                     if(error){
                         if((error.length>=1 && error[0].code==34) || (Object.keys(error).length == 0)){
                              console.log("Error or projected accounts or no tweets")
                              end_of_query_sub=true;
                              db.runAnyQuery("update twitter_users set nonexist=true where id=$1", [row.id], function(up_e, up_r){
                                    if(up_e){
                                         console.log(up_e);
                                         next_timeline(up_e);
                                         return;
                                    }
                                    else {
                                         console.log("non exist updated....");
                                         next_timeline(up_e);
                                         return;
                                    }
                              });
                              return;
                          }
                         else{
                              console.log("twitter api error"+JSON.stringify(error));
                              end_of_query_sub=true;
                              next_timeline(error);
                              return;
                          } 
                      }
                     else{
                           if(response.statusCode!=200){
                                 console.log("StatusCode is not 200:"+response);
                                 end_of_query_sub=true;
                                 next_timeline(new Error('Request not successful'));
                                 return;
                            }

                           if(response.headers['x-rate-limit-remaining']==1){
                                 sleep.sleep(60);
                            }
                           console.log("calls remaining:"+response.headers['x-rate-limit-remaining']);
                           try{
                                 json=JSON.parse((JSON.stringify(json)).replace(/\\u0000/g,""));
                              }
                           catch(pe){ 
                                 end_of_query_sub = true;
                                 console.log(pe)
                                 db.runAnyQuery('update twitter_users set last_tweet_crawled=now() where id=$1',[row.id],function(parse_err, parse_res){
                                       if(parse_err){
                                            console.log(parse_err)
                                            next_timeline(parse_err);
                                            return;
                                        }
                                       else {
                                            next_timeline(pe);
                                            return;
                                        }
                                  })
                                 return;
                              }

                           var sql;
                           var arguments=[row.id];
                           if(json.length<=1){
                                 sql="update twitter_users set last_tweet_crawled=now() where id=$1"
                                 end_of_query_sub=true;
                            }
                           else {
                                 total_tweet = total_tweet+ json.length;
                                 sql="with pre as (update twitter_users set last_tweet_crawled=now() where id=$1) insert into tweets (tweet_id, twitter_user, json) VALUES "
                                 for(var i=0;i<json.length;i++){
                                      if(i!=0){ //tweet_id, twitter_user, json
                                           sql+=","
                                      }
                                      var placeholder=arguments.length+1;
                                      sql+="($"+placeholder;
                                      if(!max_id){
                                           max_id=json[i].id;
                                      }
                                      else {
                                           if(json[i].id<max_id){
                                                max_id=json[i].id;
                                            }
                                      }
                                      arguments.push(json[i].id);
                                      sql+=", $1";
                                      placeholder=arguments.length+1;
                                      sql+=", (regexp_replace($"+placeholder+"::text, '\\u0000', '\\\\u0000', 'g'))::jsonb";
                                      arguments.push(json[i]);
                                      sql+=")";
                                  }
                                 sql+=" on conflict (tweet_id) do nothing"
                            }
                           db.runAnyQuery(sql, arguments, function(te, tr){
                                 if(te){
                                     console.log(JSON.stringify(json));
                                     next_timeline(te);
                                     return;
                                  }
                                 else {
                                     console.log("json saved.....");
                                     next_timeline();
                                     return;
                                  }
                           })
                           return;
                      } 
                 }); // getContents
            }, function next_timeline (tweet_error){
                      if(tweet_error){
                            console.log("encountering error while fetching timeline", tweet_error);
                            next_user();
                            return;
                      }
                      else{
                            console.log("all timeline fetched");
                            next_user();
                            return;
                      }
                });
            })
        }, function next_user (repoLoopError){
                      if(repoLoopError){
                          console.log(loopError);
                          throw loopError;
                          return;
                      }
                      else {
                          console.log("all tweets for available twitter accounts fetched");
                          return;
                      }
            });
  }


functions.getTweets()