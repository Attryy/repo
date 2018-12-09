var async = require('async');
var db = require('./db_write.js');
var jsonfile = require('jsonfile')

var functions = {};

// A function to collect the github infomation in to pre-defined structure from Postgre.
// Starting from the linkedin_github_link table to obtain (linkedin_id, gid, original_gid), then use gid to query profile information and the repos.
// If gid equals the original_gid, then this pair is a positive match. 
// There are around 900k github users in linkedin_github_link table, so we split these records into different files. 

functions.merge_git = function(){
var github_individual= {
            "match": null, 
            "linkedin_id": null, 
            "gid":null,
            "git_name":null,
            "git_login": null,
            "git_location": null,
            "git_company": null,
	    "git_org": null,
	    "git_email":null,
	    "git_blog": null,
	    "git_websiteUrl":null,
	    "github_url":null,
            "bio": null,
	    "repos":[]
	     };
	var sql = "select linkedin_id[1], gid from identity_link.linkedin_github_link order by linkedin_id[1] desc, gid desc limit 1 offset $1"
	var counter = 0;
	var github_id = null;
	async.until(function(){return counter >= 150000},
		function(callback){
                       counter++;
			console.log("data for "+counter+"...");
			db.runAnyQuery(sql, [counter-1], function(err,res){ 
				if(err){
					console.log(err);
					callback(err,null);
					return;
				}
			if(res.rows[0].gid == github_id)
				{
				 console.log("identical record..." +res.rows[0].gid+" == "+github_id );
                                    callback();
					return;
				}
				else{

				var rows_link= res.rows;
				var github = rows_link[0].gid;
				var linkedin = rows_link[0].linkedin_id

				var sql_github = "select links.match, links.linkedin_id[1], links.gid,links.original_gid[1], log.json from identity_link.linkedin_github_link links \
				inner join public.github_users gu \
				on gu.id = links.gid \
				inner join public.github_users_info_logs info \
				on info.gid = gu.id \
				inner join public.github_users_log log \
				on log.id = info.log_id \
				where links.gid = $1 and links.linkedin_id[1] = $2 \
				limit 1";

				db.runAnyQuery(sql_github, [github,linkedin], function(err, res){
					if(err){
						console.log(err);
						 callback(err, null);
						return;
					}
					var rows = res.rows[0];
					var git_json = rows.json;

                    if(git_json.user){
                    	git_json = git_json.user;
                    }

                    github_individual.linkedin_id = rows_link[0].linkedin_id;
                    github_individual.gid = rows.gid;

                    if(rows.original_gid == rows.gid){
                    	github_individual.match = 1;
                    }
                    else {github_individual.match = 0;}


                   if(git_json.name){
                    	github_individual.git_name= git_json.name;
                   }
                   else{github_individual.git_name= null;}

                   if(git_json.login){
                    	github_individual.git_login= git_json.login;
                    }
                   else{github_individual.git_login= null;}


                    if(git_json.location){
                    	github_individual.git_location = git_json.location;
                    }
                    else{github_individual.git_location= null;}

                    if(git_json.company){
                    	github_individual.git_company = git_json.company;
                   }
                    else{github_individual.git_company= null;}


                    if(git_json.organization){
                    	github_individual.git_org = git_json.organization.edges;
                    }
                    else{github_individual.git_org= [];}

                    if(git_json.email){
                    	github_individual.git_email = git_json.email;
                    }
                    else{github_individual.git_email= null;}

                    if(git_json.blog){
                    	github_individual.git_blog = git_json.blog;
                    }
                    else{github_individual.git_blog= null;}

                    if(git_json.websiteUrl){
                    	github_individual.git_websiteUrl = git_json.websiteUrl;
                    }
                    else{github_individual.git_websiteUrl= null;}

                    if(git_json.html_url){
                    	github_individual.github_url = git_json.html_url;
                    }
                    else{github_individual.github_url= null;}

                    if(git_json.bio){
                    	github_individual.bio = git_json.bio;
                    }
                    else{github_individual.bio= null;}

                    github_individual.repos = []; //clear history in the repos
                    if(git_json.repositories && git_json.repositories.nodes && git_json.repositories.nodes.length>0)
                    {
                    	var repo = git_json.repositories.nodes;
                    	
                    	for(var i =0; i<repo.length; i++)
                    	{
                    		var temp_result = {"name": null, "url": null, "description": null, "lang":[], "readme": null, "isFork": null}

                            if(repo[i].name) 
                            {temp_result.name= repo[i].name}

                            if(repo[i].url)
                            {temp_result.url = repo[i].url}

                            if(repo[i].readme && repo[i].readme.text)
                            {
                            	temp_result.readme= repo[i].readme.text
                            }

                            if(repo[i].description)
                            {
                            	temp_result.description = repo[i].description
                            }

                            if(repo[i].isFork)
                            {
                            	temp_result.isFork= repo[i].isFork
                            }

                            if(repo[i].languages&& repo[i].languages.edges && repo[i].languages.edges.length>0)
                            {
                            	for(var j =0; j < repo[i].languages.edges.length; j++)
                            	{
                            		if(repo[i].languages.edges[j].node && repo[i].languages.edges[j].node.name)
                            		{
                            			temp_result.lang.push(repo[i].languages.edges[j].node.name)
                            		}
                            	}
                            }

                            github_individual.repos.push(temp_result);
                    	}
                    }
                   

                    var file_github = './github_data_15.json'
                    
                     callback(null, github_individual);
                     console.log("writing github user info for " +counter+":"+ github_individual.git_login +"...");
                     jsonfile.writeFile(file_github, github_individual, {flag: 'a'}, function (err) {
                     	if(err)
                          { console.error(err);}
                           })

                     github_id= github;
                      
                  }) 

                 return;
             }
		 }) 
               
            }, function callback(err, result){
            	if(err){
            		console.log(err);
            		throw err;
            		return;

            	}      
            })
          }

functions.merge_git()
