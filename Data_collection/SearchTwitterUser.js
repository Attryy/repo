 var async = require('async');
 var sleep = require('sleep');


 var functions = {};
 var TwitterBot = require('./TwitterBot.js')
 var db = require('./db_write.js');
 var Shared = require('./shared.js');
 var shared = new Shared();

 var counter = 0;
 functions.searchAddNewUsers=function(){
 	var sql_linkedin = 'select name, twitter[1], linkedin[1] from identity_link.linkedin_twitter_profiles \
 	where name is not null and good_name = true and twitter_byname_searched = false order by linkedin [1] asc offset $1 limit 1 '
    var end_of_query = false;
    async.until(function(){return end_of_query},
    	function(callback){ 
            counter++;
    		db.runAnyQuery(sql_linkedin, [counter], function(error, response){ 
    			if(error){
    				console.log(error);
    				throw error;
    				return;
    			}
    			if(response.rowCount == 0){
    				console.log("All available names searched.");
    				end_of_query =true;
    				callback();
    				return;
    			}

    			var rows = response.rows;
    			var name = rows[0].name;
    			var patt = new RegExp(/[^\x00-\x7F]+/g); //check for non-ASCII characters

    			if(!name || name.indexOf('&') >-1|| name == ""  || name == "''"|| patt.test(name)){
    				console.log("bad name...");
    				db.runAnyQuery("update identity_link.linkedin_twitter_profiles set good_name = false where linkedin[1] = $1", [rows[0].linkedin], function(error_pro, response_pro){
    					if(error_pro){
    						console.log(error_pro);
    						throw error_pro;
    						return;
    					}
    					console.log('Bad name profile updated...');
    					callback();
    					return;
    				})    
    			}
    			var good_name = shared.removeDiacritics(name).replace('@','')
    			console.log("good name:"+good_name);

    			var params =  {"q": good_name , "page":1, "count": 20, 'include_entities':true} // search parameters
                var bot = new TwitterBot();
                bot.getContent("users/search", params, function(err, jsonData, res){
                     if (err){
                         if(err[0].code == 88){
                             console.log('Hitting search limit. About to sleep...');
                             sleep.sleep(60);
                         }
                         else{
                             console.log(err[0]);
                             callback(new Error('Unknown error'));
                             return;
                         }
                     }
                     if(!res.headers){
                           console.log('Header missing: '+res.headers);
                           callback(new Error('Header missing'));
                           return;   
                     }
                     console.log("x-rate-limit-remaining:"+ res.headers['x-rate-limit-remaining']);

                     if(res.headers['x-rate-limit-remaining']== 0){
                          console.log('Hitting the rate limit, about to sleep...');
                          sleep.sleep(60);
                     }
                     if(Object.keys(jsonData).length < 1){
                          console.log('No results found for this name.');
                          
                          db.runAnyQuery('update identity_link.linkedin_twitter_profiles set twitter_byname_searched = true where twitter[1] = $1', [rows[0].twitter], function(err_up, res_up){
                             if (err_up){
                                 console.log(err_up);
                                 throw err_up;
                                 return;
                             }
                             else{
                                console.log("No search results updated for profile table....");
                                callback();
                                return;
                             }
                          })
                     }

                     else{
                          var arguments = [];

                          var sql_insert = 'insert into twitter_users(link, last_crawled, nonexist, json) VALUES '
                          for (var i = 0; i<jsonData.length; i++){
                              if(i != 0){
                                  sql_insert += ","
                              }

                              var placeholder = arguments.length+1;
                              sql_insert += "($"+placeholder;
                              arguments.push(jsonData[i].screen_name);
                              arguments.push(jsonData[i]);
                              sql_insert += ", now(), false,";
                              placeholder = arguments.length; 
                              sql_insert += "(regexp_replace($"+placeholder+"::text, '\\\\u0000','','g'))::jsonb)"
                          }
                          sql_insert += " on conflict (link) do update set link = excluded.link, last_crawled = now() returning id"

                          db.runAnyQuery(sql_insert, arguments, function(err_in, res_in){
                             // console.log(sql_insert)
                              if(err_in){
                                   console.log(err_in);
                                   callback(err_in);
                                   return;
                              }
                              else{
                                   insert_res = res_in.rows;
                                   //console.log(insert_res[0].id);
                                   var sql_link = 'insert into identity_link.linkedin_twitter_link (linkedin_id, tid, original_tid,timestamp) VALUES '
                                   var arg_link = [rows[0].linkedin, rows[0].twitter];
                                   for (j=0; j<insert_res.length; j++){
                                       if(j != 0){
                                           sql_link += ","
                                        }
                                       sql_link+= "($1,";
                                       var place_link = arg_link.length+1;
                                       arg_link.push(insert_res[j].id);
                                       sql_link += "$"+place_link+",$2, now())"
                                    }
                                   sql_link += ' on conflict on CONSTRAINT linkedin_twitter_link_pkey do nothing returning tid'

                                   db.runAnyQuery(sql_link, arg_link, function(err_link, res_link){
                                         if(err_link){
                                             console.log(err_link);
                                             callback(err_link);
                                             return;
                                         }
                                         else{
                                             console.log('All search results collected and link table updated.')
 
                                             db.runAnyQuery('update identity_link.linkedin_twitter_profiles set twitter_byname_searched = true where twitter[1] = $1', [rows[0].twitter], function(err_up, res_up){
                                                   if (err_up){
                                                      console.log(err_up);
                                                      throw err_up;
                                                      return;
                                                    }
                                                  else{
                                                      console.log('Profile table updated.')
                                                      callback();
                                                      return;
                                                    }
                                             })
       
                                         }
                                   })

                                }

                           })
                       }; //insert runANyquery
                  })// bot 
            });
}, function(error){
        if(error){
            console.log(error);
            throw error;
            return;
        }
        else{
           console.log("All name searched.");
        }
    });
}

functions.searchAddNewUsers();
