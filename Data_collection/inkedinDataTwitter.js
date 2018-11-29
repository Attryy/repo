var async = require('async');
var db = require('./db_write.js');
var jsonfile = require('jsonfile')

var functions = {};

//This function extract Linkedin data from POstgres database and combine useful information into a pre-defined structure. 
//Starting with the linkedin_twitter_profile, we use the name to search linkedin_user table joined with the profile table by canonical_url to obtain the detailed information.
//Wher there is indeed profile information for this user, then we parse the JSON data into the format we want. Otherwise, we collect the URL's for the linkedin users who appear 
//in the linkedin_twitter_profiles table yet with no profile details in the profile table. 


functions.merge_linkedin = function(){
    var sql = "select distinct(linkedin[1]) from identity_link.linkedin_twitter_profiles order by linkedin[1] desc limit 1 offset $1"
    var counter =0;
    var end_of_query = false
    async.until(function(){return end_of_query},
        function(next_linkedin){
            counter++;
            console.log("---linkedin data for #"+counter+" user ---");
            db.runAnyQuery(sql, [counter-1], function(err,res){ 
                if(err){
                    console.log(err);
                    next_linkedin(err);
                    return;
                }
                if(res.rowCount == 0){
                    console.log("All available Linkedin profile searched.");
                    end_of_query = true;
                    next_linkedin();
                    return;
                    }

                var rows_link= res.rows;
                var linkedin = rows_link[0].linkedin;
                var sql_linkedin = 'with linkedin AS(select * from public.linkedin_users lu where lu.id = $1 limit 1)\
                                 select profiles.json from profiledb_profiles profiles \
                                 where profiles.canonical_url = (select canonical_url from linkedin)\
                                 limit 1'
                db.runAnyQuery(sql_linkedin, [linkedin], function(error, linkedin_res){
                    if(error){
                         console.log(error);
                         next_linkedin(error);
                         return;
                    }
                //console.log(rows_link[0].linkedin);
                // console.log(JSON.stringify(linkedin_res));

                    else if(linkedin_res.rowCount == 0 ){
                                console.log("no linkedin information...");
                                db.runAnyQuery('select * from public.linkedin_users lu where lu.id = $1 limit 1', [linkedin], function(err_no, result){
                                        if(err_no){
                                             console.log(err_no);
                                             next_linkedin(err_no);
                                             return;
                                        }
                                        if(result.rowCount == 0){
                                             console.log("no record for this user....");
                                             next_linkedin();
                                             return;
                                        }
                                        var file_linkedin_missing = './missing_linkedin_data_twitter.json'
                    
                                        next_linkedin(null, result.rows[0].canonical_url);
                                        console.log("---- missing linkedin user info: " + rows_link[0].linkedin+"---");
                                        jsonfile.writeFile(file_linkedin_missing, result.rows[0].canonical_url, {flag: 'a'}, function (err_missing) {
                                              if(err_missing){
                                                  console.error(err_missing);
                                                  next_linkedin(err_missing);
                                                  return;
                                              }
                                              else {
                                                  next_linkedin();
                                                  return;
                                              }
                                        })
                                })
                    }
                    else{
                         var linkedin_ind={
                                "linkedin_id": null,
                                "full_name": null,
                                "linkedin_url": null,
                                "headline": null,
                                "industry": null,
                                "location": null,
                                "education":null,
                                "experience":null,
                                "interests":null,
                                "summary":null,
                                "projects":null,
                                "publications":null,
                                "skills": null,
                                "websites": null,
                                "languages": null,
                                "num_connections": null 
                          };
                         //console.log("this is the linkedin info"+ JSON.stringify(res));
                         var rows = linkedin_res.rows[0];
                         var li_json = rows.json;

                         linkedin_ind.linkedin_id = linkedin;


                         if(li_json.full_name){
                                linkedin_ind.full_name = li_json.full_name;
                          }

                         if(li_json.canonical_url){
                                linkedin_ind.linkedin_url = li_json.canonical_url;
                          }

                         if(li_json.headline){
                                 linkedin_ind.headline = li_json.headline;
                          }

                         if(li_json.industry){
                                 linkedin_ind.industry = li_json.industry;
                          }

                         if(li_json.locality){
                                 linkedin_ind.location = li_json.locality;
                          }
            
                         if(li_json.education){
                                linkedin_ind.education = [];
                                var edu = li_json.education;
                                for(i = 0; i <edu.length; i++){
                                        var temp_edu = {"name":null, "major": null, "summary": null};
                                        if(edu[i].name){
                                              temp_edu.name = edu[i].name;
                                        }

                                        if(edu[i].major){
                                              temp_edu.major = edu[i].major;
                                        }

                                        if(edu[i].summary){
                                              temp_edu.summary = edu[i].summary;
                                        }

                                        linkedin_ind.education.push(temp_edu);

                                }
                         }      
                         if (li_json.experience){
                                linkedin_ind.experience = [];
                                var exp = li_json.experience;
                                for (j =0; j< exp.length; j++){
                                        var temp_exp = {"title": null, "description": null, "organization": null};
                                        if(exp[j].title){
                                              temp_exp.title = exp[j].title
                                        }
                                        if(exp[j].description){
                                              temp_exp.description = exp[j].description;
                                        }
                                        if(exp[j].organization){
                                              temp_exp.organization = exp[j].organization[0].name;
                                        }

                                        linkedin_ind.experience.push(temp_exp);
                                        }
                         }
                         if(li_json.interests){
                                linkedin_ind.interests = li_json.interests;
                         }

                         if(li_json.summary){
                                linkedin_ind.summary = li_json.summary;
                         }
                         if(li_json.projects){
                                linkedin_ind.projects = [];
                                var proj = li_json.projects;
                                for( i =0; i< proj.length; i++){
                                        var temp_project = {"title": null, "description": null}

                                        if(proj[i].name){
                                            temp_project.name = proj[i].name;
                                        }

                                        if(proj[i].description){
                                            temp_project.description= proj[i].description;
                                        }

                                        linkedin_ind.projects.push(temp_project)

                                        }
                         } 
                         if(li_json.publications){
                                linkedin_ind.publications= [];
                                var pub = li_json.publications;
                                for(i=0; i< pub.length; i++){
                                        var temp_publication = {"title": null, "summary": null};

                                        if(pub[i].title){
                                             temp_publication.title = pub[i].title;
                                        }

                                        if(pub[i].summary){
                                             temp_publication.summary = pub[i].summary;
                                        }

                                        linkedin_ind.publications.push(temp_publication);


                                }
                         }

                         if(li_json.skills){
                                linkedin_ind.skills= li_json.skills;
                         }

                         if(li_json.websites){
                                linkedin_ind.websites = li_json.websites;
                         }

                         if(li_json.languages){
                                linkedin_ind.languages = []
                                for(var j = 0; j< li_json.languages.length; j++){
                                            linkedin_ind.languages.push(li_json.languages[j].name)
                                }
                         }

                         if(li_json.num_connections){
                                 linkedin_ind.num_connections = li_json.num_connections
                         }


                         var file_linkedin = './linkedin_data_twitter.json'
                         console.log("--- linkedin user info: " + linkedin_ind.full_name+"---");
                         jsonfile.writeFile(file_linkedin, linkedin_ind, {flag: 'a'}, function (err_write) {
                             if(err_write){
                                   console.error(err_write)
                                   next_linkedin(err_write);
                                   return;
                             }
                             else{
                                  next_linkedin();
                             }

                         });
                    } 
                     
                }) 
            }) 
                
    }, function next_linkedin (err, result){
            if(err){
                 console.log(err);
                 throw err;
                 return;
             }
       })
}
 
                


functions.merge_linkedin()
