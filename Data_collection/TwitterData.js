var async = require('async');
var db = require('./db_write.js');
var jsonfile = require('jsonfile')

var functions = {};

// A function to collect the twitter infomation in to pre-defined structure from Postgresql database.
// Starting from the linkedin_twitter_link table to obtain (linkedin_id, tid, original_tid), then use tid to query profile information and the tweets.
// If tid equals the original_tid, then this pair is a positive match. 
// There are around 900k twitter users in linkedin_twitter_link table, so we split these records into different files to accelerate the process. 

functions.merge_twitter = function(){
	var sql = "select linkedin_id, tid from identity_link.linkedin_twitter_link where collected = false and tid < 4225000 order by linkedin_id asc, tid desc  limit 1 offset $1"
	var counter = 0;
	var twitter_id = null;
	async.until(function(){return counter >= 100000},
		 function(next_pair){
            var twitter_individual= {
                 "match": null, 
                 "linkedin_id": null, 
                 "tid":null,
                 "twitter_name":null,
                 "screen_name": null,
                 "twitter_location": null,
                 "description": null,
                 "lang":null,
                 "twitter_url": null, //expanded url in entities object 
                 "friends_count":null,
                 "followers_count":null,
                 "status_counts": null,
                 "profile_image_url": null, //profile_image_url_https
                 "tweets":null
            };
            counter++;
			      db.runAnyQuery(sql, [counter-1], function(e_link,r_link){ 
				          if(e_link){
				             	console.log(e_link);
					            next_pair(e_link);
					            return;
				          }
			            if(r_link.rows[0].tid == twitter_id){
				              console.log("identical record..." +res.rows[0].tid+" == "+twitter_id );
                      next_pair();
				              return;
				          }
			            else{  
                      console.log("---collecting data for #"+counter+" pair---, "+ 'Linkedin:'+ r_link.rows[0].linkedin_id+ ", twitter: "+r_link.rows[0].tid );
 
				              var rows_link= r_link.rows;
				              var twitter = rows_link[0].tid;
			              	var linkedin = rows_link[0].linkedin_id

			              	var sql_twitter = "select links.linkedin_id, links.tid, links.original_tid, tu.link, tu.json, tw.json as tweets from identity_link.linkedin_twitter_link links\
                           inner join public.twitter_users tu on tu.id = links.tid left join public.tweets tw on\
                           tw.twitter_user = tu.id where links.linkedin_id = $1 and links.tid = $2 and last_tweet_crawled is not null and tu.nonexist = false order by tw.tweet_id desc limit 500" 

				              db.runAnyQuery(sql_twitter, [linkedin, twitter], function(err, res){
					                 if(err){
					                   	console.log(err);
					                    next_pair(err);
					                    return;
				                  	}
                           else {
                              if(res.rowCount == 0){
                                   console.log("No complete information...")
                                   next_pair();
                                   return;
                              }
                              else{
                                   var profile = res.rows[0] // duplicate profile data for the same user
                                   var result = res.rows
                                   var tu_json = profile.json;

                                   if(!tu_json){
                                          console.log("No profile data for this twitter user: " + twitter);
                                          next_pair(new Error("Empty JSON"));
                                          return;
                                    }

                                   else {
                                          if(tu_json.error){
                                               console.log("Error fetching user profile: the user could be suspended");
                                               next_pair(new Error("Error in JSON"));
                                               return;
                                          }
                                          else{

                                               twitter_individual.linkedin_id = profile.linkedin_id;
                                               twitter_individual.tid = profile.tid;

                                               if(profile.original_tid == profile.tid){
                                                     twitter_individual.match = 1;
                                                }
                                               else {twitter_individual.match = 0;}


                                               if(tu_json.name){
                                                     twitter_individual.twitter_name= tu_json.name;
                                                }

                                               if(tu_json.screen_name){
                                                     twitter_individual.screen_name= tu_json.screen_name;
                                                }


                                               if(tu_json.location){
                                                     twitter_individual.twitter_location = tu_json.location;
                                                }

                                               if(tu_json.lang){
                                                     twitter_individual.lang= tu_json.lang;
                                                }

                                               if(tu_json.description){
                                                     twitter_individual.description = tu_json.description;
                                                }

                                              if(tu_json.entities && (tu_json.entities.url || tu_json.entities.description.urls[0]) ){
                                                     //console.log(JSON.stringify(tu_json.entities))
                                                     twitter_individual.twitter_url = [];

                                                     if(tu_json.entities.url){
                                                          for (var l =0; l < tu_json.entities.url.urls.length; l++){
                                                               twitter_individual.twitter_url.push(tu_json.entities.url.urls[l].expanded_url)
                                                          }
                                                      }
                                                     if(tu_json.entities.description && tu_json.entities.description.urls.length>0){
                                                         for (var k =0; k < tu_json.entities.description.urls.length; k++){
                                                               twitter_individual.twitter_url.push(tu_json.entities.description.urls[k].expanded_url)
                                                          }
                                                      }

                                              }

                                             if(tu_json.friends_count){
                                                   twitter_individual.friends_count = tu_json.friends_count;
                                              }

                                             if(tu_json.followers_count){
                                                   twitter_individual.followers_count = tu_json.followers_count;
                                              }

                                             if(tu_json.statuses_count){
                                                    twitter_individual.status_counts = tu_json.statuses_count;
                                              }

                                             if(tu_json.profile_image_url_https){
                                                    twitter_individual.profile_image_url = tu_json.profile_image_url;
                                              }
                        
                                             if(result[0].tweets){
                                                    console.log("Tweets available")
                                                    twitter_individual.tweets = [];
                                                    for(var i =0; i< res.rowCount; i++){
                                                         var temp_result = {"text": null,"lang": null, "isRetweet": 0, "is_quote_status": null, "hashtags": null, "user_mentions": null, "url": null, "coordinates": null, 
                                                                            "geo_name": null}
                                                         if(result[i].tweets.text){
                                                             temp_result.text = result[i].tweets.text
                                                          }

                                                         if(result[i].tweets.lang){
                                                             temp_result.lang = result[i].tweets.lang
                                                          }

                                                         if(result[i].tweets.retweeted_status){
                                                             temp_result.isRetweet = 1
                                                          }

                                                         if(result[i].tweets.is_quote_status == false){
                                                             temp_result.is_quote_status = 0
                                                          }
                                                         else{temp_result.is_quote_status = 1}

                                                         if(result[i].tweets.entities.hashtags.length >0 ){
                                                                temp_result.hashtags = [];
                                                                for (var j =0; j<result[i].tweets.entities.hashtags.length; j++ ){
                                                                       temp_result.hashtags.push(result[i].tweets.entities.hashtags[j].text)
                                                                 }
                                                          }

                                                         if(result[i].tweets.entities.user_mentions.length >0 ){
                                                                temp_result.user_mentions = [];
                                                                for (var m = 0; m < result[i].tweets.entities.user_mentions.length; m++){
                                                                       temp_result.user_mentions.push(result[i].tweets.entities.user_mentions[m].name)
                                                                 }
                                                          }
                                                         if(result[i].tweets.entities.urls.length >0 ){
                                                                temp_result.url = [];
                                                                for (var n = 0; n < result[i].tweets.entities.urls.length; n++){
                                                                       temp_result.url.push(result[i].tweets.entities.urls[n].expanded_url)
                                                                  }
                                                          }

                                                         if(result[i].tweets.coordinates){
                                                                temp_result.coordinates = result[i].tweets.coordinates
                                                          }

                                                         if(result[i].tweets.place){
                                                                temp_result.geo_name = result[i].tweets.place.full_name
                                                          }

                                                         twitter_individual.tweets.push(temp_result)
 
                                                    }
                                              }
                                             var file_twitter = './data/twitter_data.json'
                                             jsonfile.writeFile(file_twitter,twitter_individual,{flag: 'a'}, function (write_err){
                                                   if(write_err){
                                                        console.log(write_err);
                                                        return;
                                                    }
                                                   else{
                                                        sql_collect = 'update identity_link.linkedin_twitter_link set collected = true where linkedin_id = $1 and tid = $2'
                                                        db.runAnyQuery(sql_collect, [linkedin, twitter], function(err_up, res_up){
                                                              if(err_up){
                                                                    console.log('Update link table error');
                                                                    next_pair(err_up);
                                                                    return;
                                                              }
                                                              else{
                                                                    console.log('---collected---'+twitter_individual.twitter_name);
                                                                    twitter_id = twitter;
                                                                    next_pair();
                                                                    return;
                                                              }
                                                        })
                                                    }
                                              })
                                          }
                                    }
 
                               }
                            }
                      })
                  }
            }) 
      }, function next_pair (Looperr){
             if(Looperr){
                 console.log(Looperr);
                 throw(Looperr);
                 return;
              }
          });
}

			

functions.merge_twitter()
