var async = require('async');
var unirest = require('unirest');
var fs = require('fs');
var github = require('octonode');
var sleep = require('sleep')
var parse = require('parse-link-header')
var parseFullName = require('parse-full-name').parseFullName;
var urlencode = require('urlencode');
var sleep = require('sleep');



 var functions = {};
 var GithubDatabot= require('./GithubDatabot');
 var Shared = require('./shared.js');
 var ElasticSearch = require('./elasticsearch.js');
 var es = new ElasticSearch();

 var db = require('./db_write.js');
 var databot = new GithubDatabot(); 
 var shared = new Shared();

functions.parseV4UserInfo=function(data)
  {
    var result={
      "id":null,
      "org":null,
      "name":null,
      "first_name":null,
      "last_name":null,
      "link":null,
      "email":null,
      "login":null,
      "valid":false
    }
    if(!data || !data.user)
    {
      return null;
    }
    const user=data.user

    if(user.id)
    {
      result.id=Buffer.from(user.id, 'base64').toString('ascii').replace(/.*User/g,"");
      result.id=Number(result.id);

      if(isNaN(result.id))
      {
        result.id=null;
      }
      else {
        result.valid=true;
      }
    }
    if(user.company)
    {
      user.org=user.company;
    }
    if(user.name)
    {
      result.name=user.name
      name = parseFullName(user.name);
      result.first_name=name.first;
      result.last_name=name.last;
    }
    if(user.blog)
    {
        result.link=user.blog
    }
    if(user.email)
    {
      result.email=user.email
    }
    if(user.login)
    {
      result.login=user.login
      result.valid=true;
    }
    return result;

  }

 functions.addNewUserV4=function(login, callback)
  {

    var ghuser = databot.userV4(login);
    console.log("preparing to get user info.....");
    ghuser.info(function(err, data, headers){

      if(!headers || !headers['x-ratelimit-remaining'])
      {
        console.log("header missing:"+headers+" err:"+err);
        callback(null,"missing header");
        return;


      }

      if(err)
      {
        console.log('github error: '+err);
        callback(null,err);
        return;
      }
      console.log("\n limit-remaining:"+headers['x-ratelimit-remaining']);
      if(headers['x-ratelimit-remaining']==1)
      {
        console.log("hitting limit, about to sleep");
        sleep.sleep(60);
      }


      if(!data || !data.user)
      {
        console.log('no data');
        console.log('login '+login);
        callback(null);
        return;
      }
      console.log("about to parse data....");
      try{

          data=JSON.parse((JSON.stringify(data)).replace(/\\u0000/g,"").replace(/\n/g, "\\n").replace(/\r/g,"\\r").replace(/\t/g,"\\t").replace(/\f/g, "\\f"));
      }   
      catch(pe)
      {
        console.log(pe)
        callback(null,"no valid information")
        return;
      }

      parsed_user_info=functions.parseV4UserInfo(data);
      data=data.user;

      if(!parsed_user_info || !parsed_user_info.valid)
      {
        callback(null,"no valid information for parsed data")
        return;
      }
      var readme_processed=(data.repositories && data.repositories.nodes &&  data.repositories.nodes.length>0)
      var sql="with log_insert as (insert into public.github_users_log ( json) values (($1)::jsonb) returning id), \
              update_guser as (insert into public.github_users (github_id, org,  name, link,  first_name, \
                last_name, email, login, readme_processed, last_crawled, last_user_detail_check, last_readme_fetch, last_v4api_check, lastest_v4_info) select\
                $2, $3, $4, $5, $6, $7, $8, $9, $10, now(), now(), now(), now(), id from log_insert on conflict (github_id) do update \
                set readme_processed=excluded.readme_processed, last_crawled=now(), last_user_detail_check=now(), last_readme_fetch=now(),last_v4api_check=now(), lastest_v4_info=excluded.lastest_v4_info returning id, lastest_v4_info) \
                insert into public.github_users_info_logs(gid, log_id) select id, lastest_v4_info from update_guser returning gid"
      var args=[data, parsed_user_info.id, parsed_user_info.org, parsed_user_info.name,
                   parsed_user_info.link, parsed_user_info.first_name,
                   parsed_user_info.last_name,
                   parsed_user_info.email,
                  parsed_user_info.login,
                  readme_processed]

      console.log("\n inserting user --> ", parsed_user_info);
      db.runAnyQuery(sql, args, function(uer, ures){
          if(uer)
          {
            console.log(uer);
            throw uer;
          }
          else {
            if(ures.rows.length==0 || !ures.rows[0].gid)
            {
              console.log(parsed_user_info)
              throw "no rows inserted";
            }
            else {
                console.log("user "+ures.rows[0].gid+" processed, moving on");
                callback(ures.rows[0].gid);
                console.log("need to process repos:", readme_processed)
            }
            return;
          }
      });

    });
  }

  functions.parseGithubRepoInfo=function(repo)
  {

    var result={

      "repo_id":null,
      "name":null,
      "url":null,
      "description":null,
      "lang":null,
      "v4":true,
      "valid":false
    }
    var es_json={
      "id":null,
      "urlname":null,
      "name":null,
      "description":null,
      "lang":[],
      "readme":null
    }



    if(repo.id)
    {
      result.repo_id=Buffer.from(repo.id, 'base64').toString('ascii').replace(/.*Repository/g,"");
      result.repo_id=Number(result.repo_id);

      if(isNaN(result.repo_id))
      {
        result.repo_id=null;
      }
      else {
        es_json.id=result.repo_id;
        result.valid=true;
      }
    }

    if(repo.name)
    {
      result.name=repo.name;
      es_json.name=repo.name
    }
    if(repo.description)
    {
        result.description=repo.description;
        es_json.description=repo.description
    }
    if(repo.url)
    {
      result.url=repo.url
      es_json.urlname=repo.url
    }
    if(repo.primaryLanguage)
    {
      result.lang=repo.primaryLanguage
    }
    if(repo.languages && repo.languages.edges && repo.languages.edges.length>0)
    {
      for(var i=0;i<repo.languages.edges.length;i++)
      {
        if(repo.languages.edges[i].node && repo.languages.edges[i].node.name)
        {
            es_json.lang.push(repo.languages.edges[i].node.name);
        }

      }
    }

    if(repo.readme && repo.readme.text)
    {
      es_json.readme=repo.readme.text;
    }

    return {"table":result, "es":es_json};
  }

  functions.processUserRepos=function(gid, repos)
  {
    if(!gid || !repos || repos.length==0)
    {
      return;
    }
    var sql="with pre as (insert into github_repos (repo_id, name, url, description, json, lang, v4, last_uploaded_to_es)  VALUES "
    var es_upsert=[]
    var args=[gid];
    for(var i=0;i<repos.length;i++)
    {
      parsedInfo=functions.parseGithubRepoInfo(repos[i]);
      if(parsedInfo.es)
      {
          es_upsert.push(parsedInfo.es);
      }
      if(parsedInfo.table)
      {

        if(args.length>1)
        {
          sql+=", "
        }
        args.push(parsedInfo.table.repo_id)
        sql+="($"+args.length;

        args.push(parsedInfo.table.name)
        sql+=", $"+args.length;

        args.push(parsedInfo.table.url)
        sql+=", $"+args.length;

        args.push(parsedInfo.table.description)
        sql+=", $"+args.length;

        args.push(repos[i])
        sql+=", (regexp_replace($"+args.length+"::text, '\\u0000', '', 'g'))::jsonb";

        args.push(parsedInfo.table.lang)
        sql+=", $"+args.length;

        sql+=", true, now())";
      }
    }
    sql+=" on conflict (repo_id) do update set v4=true, last_uploaded_to_es=now(), json=excluded.json returning repo_id)\
          insert into github_users_repos(gid, rid) select $1, repo_id from pre on conflict on CONSTRAINT gid_rid_unique do nothing"
    es.bulkUploadRepos(es_upsert, function(){
       console.log("elasticsearch updated, moving to process inserting sql");
       db.runAnyQuery(sql, args, function(e, r){
         if(e)
         {
           console.log(e, " || ", sql);
           throw e;
         }
         else {
           console.log("repo processed");
         }
       })
    })
  }



//This function is to search all potential github matches for a linkedin account using names. 
//We start with (name. github(id), linkedin(id)) from the linkedin_github_profiles table and create three new columns with boolean data: good_name, github_byname_searched and too_many_users.
//The values in the good_name column indicate whether or not the name can be represented by ASCII characters; 
//The values in github_byname_searched is true if the name is already searched;
//We label the too_many_users as true if there are more than 500 search results for a single name. 
//So we select a name from the linkedin_github_profiles, first check if this name can be represented by ASCII characters: if not, update the column value and return;
//If it's a good name, we use the seach method in GithubBot.js to search potential profiles and also check if the total number of results exceeds 500: if yes, update the too_many_user column and return;
//Else, insert into the github_users table using addNewuserV4 method for each one of the results with their logins and insert the (linkedin_id, gid, original_gid) into the linkedin_github_link table to keep track of the matches. Once it finishes inserting new users and link tables, it'll move on to the next name. 

functions.searchGithubUsersFromLinkedin = function()
{

	var sql = "select name, github, linkedin from identity_link.linkedin_github_profiles TABLESAMPLE SYSTEM(9) where name is not null and good_name = true and github_byname_searched = false and too_many_users = false limit 1 "
    var end_of_query = false;
    async.until(function(){return end_of_query},
    	function(callback){
    		db.runAnyQuery(sql, null, function(e,r){
    			if(e)
    			{
    				console.log(e);
    				callback(e);
    				return;
    			}
    			if(r.rowCount == 0)
    			{
    				console.log("all available names searched");
    				end_of_query = true;
    				callback();
    				return;
    			}
    			var rows=r.rows;
    			var name = rows[0].name;
    			var patt = new RegExp(/[^\x00-\x7F]+/g);
    			// take care of special characters 
    			if (!name || name.indexOf('&')>-1 || name == "" || patt.test(name))
    			{
    			     console.log("bad name...");
    		             db.runAnyQuery("update identity_link.linkedin_github_profiles set good_name = false where linkedin= $1" ,[rows[0].linkedin], function(ue, ur){
    					if (ue){
    						console.log(ue);
    						throw ue;
    						return;
    					}
    					callback();
    					return;
    				});
    		         return;
    			}
    			var good_name = shared.removeDiacritics(name).replace('@','')
    			console.log("good name:"+good_name);

    			var ghsearch = databot.search();
    			ghsearch.users(query = good_name +'in:fullname',per_page = 100,page = null,function(err, data, headers){

    				if(err && err != "too many potential matches...")
    				{
    					throw err;
    					callback(err);
    					return;
    				}
    				else{
               if(err== "too many potential matches...")
               {

                        console.log('Too many potential matches, will trigger Github abuse detection mechanism...');
                        db.runAnyQuery("update identity_link.linkedin_github_profiles set too_many_users= true where linkedin = $1", [rows[0].linkedin], function(ue, ur){
                          if (ue){
                            console.log(ue);
                            throw ue;
                            return;
                          }
                          callback();
                          return;
                        })
                        return;
      
               }
    					if(!headers|| !headers ['x-ratelimit-remaining'])
    					{
    						console.log("header missing:"+headers +"err:"+err);
    						callback(new Error('missing header'));
    						return;
    					}
    					console.log("x-ratelimit-remaining:"+headers["x-ratelimit-remaining"]);
    					if(headers['x-ratelimit-remaining']==0)
    					{
                                                console.log("hitting limit, start to sleep...");
    						sleep.sleep(60);
    					}

    					if(!data)
    					{
    						console.log("missing data")
    						callback("missing data");
    					}
    					else {
    						if(data.total_count>0)
    						{
    							console.log("there are "+data.total_count+" github profiles matches the name\n");
    								async.eachSeries(data.items, function(item, next_item){

    								functions.addNewUserV4(item.login, function(gid, error){
    									if(error)
    									{
                                                                           if(error!== 'no valid information')
    										{throw error;
    										next_item(error);
    										return;}

                                                                else{ 
                                                                 next_item(); //move on to the next iterm for JSON parse error
                                                                 return;
                                                                     }
    							       	}
    									if(gid == null){
    										next_item();
    										return;
    									}
    									else{
    										var sql ="insert into identity_link.linkedin_github_link(linkedin_id, gid, original_gid, timestamp)\
    										VALUES ($1, $2, $3, now()) on conflict on CONSTRAINT linkedin_github_link_pkey do nothing returning gid" 
    										var githubNew_id = gid; 
    										console.log(githubNew_id);
    										var args =[rows[0].linkedin, githubNew_id, rows[0].github]
    										console.log("\n inserting id into linking table..."+rows[0].linkedin +","+githubNew_id);
    										db.runAnyQuery(sql, args, function(uer, ures){
    											if(uer){
    												console.log(uer);
    												throw uer;
    											}
    											else{
    												console.log("\n updated linking table");
    											
                          next_item();
                          return;} 
    										})
    						
    									};

    								});

    							}, function(looitemerr){
    								if(looitemerr){
    									console.log('Error:'+looitemerr);
    									throw looitemerr;
    								} else{

    									db.runAnyQuery("update identity_link.linkedin_github_profiles set github_byname_searched =true where github = $1", [rows[0].github], function(ue, ur){
    										if(ue)
    										{
    											console.log(ue);
    											throw ue;
    											return;
    										}
    										console.log('all users of this particular name added, next name...');
    										callback();
    										return;
    									});
    								}
    							});
    							return;
    							
    						}
    						else {
    							console.log("all users of this particular name searched, no user found, moving on to the next name...")
    							db.runAnyQuery("update identity_link.linkedin_github_profiles set github_byname_searched= true where github=$1", [rows[0].github], function(ue,ur){
    								if(ue)
    								{
    									console.log(ue);
    									throw ue;
    									return;
    								}
    								callback();
    								return;
    							});
    						}

    					}
    				}
    			});
    		});
    	},
    	function(error){
    		if(error)
    		{
    			console.log(error);
    			throw error;
    			return;
    		}
    		else{
    			console.log("all name searched")
    		}
    	});
    	}   

functions.searchGithubUsersFromLinkedin();




