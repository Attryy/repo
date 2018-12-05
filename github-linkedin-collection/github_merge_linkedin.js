var async = require('async');
var db = require('./db_write.js');
var jsonfile = require('jsonfile')

var functions = {};

// This function is to collect the linkedin profiles that form the known matches in the linkedin_github_profiles table. 
// Starting with querying the linkedin_github_profiles, we first obtain the linkedin_id of the profile. Then using this id to link linkedin_users 
// with the profiles table in profiles database by canonical_url to get all profile details for each user. 
// In the meantime, there are users that we have id in the linkedin_users table yet without corresponding JSON in profiles table. We put down the canonical URL's 
// for these cases. 

functions.merge_linkedin = function(){

 var linkedin_ind={
                        "linkedin_id": null,
                        "full_name": null,
                        "linkedin_url": null,
                        "headline": null,
                        "industry": null,
                        "location": null,
                        "education":[],
                        "experience":[],
                        "interests":null,
                        "summary":null,
                        "projects":[],
                        "publications":[],
                        "skills": null,
                        "websites": null
                 };
        var sql = "select distinct(linkedin[1]) from identity_link.linkedin_github_profiles order by linkedin[1] desc limit 1 offset $1"
        var counter = 0;
        async.until(function(){return counter >= 180000},
                function(callback){
                        counter++;
                        console.log("data for "+counter+"...");
                        db.runAnyQuery(sql, [counter-1], function(err,res){ 
                                if(err){
                                        console.log(err);
                                        callback(err,null);
                                        return;
                                }
                             if(res.rowCount == 0)
                             {
                                   console.log("no available linkedin_id.");
                                     callback();
                                      return;
                              }

                                var rows_link= res.rows;
                                var linkedin = rows_link[0].linkedin;

                var sql_linkedin = 'with linkedin AS(select * from public.linkedin_users lu where lu.id = $1 limit 1)\
                                 select profiles.json\
                                 from profiledb_profiles profiles \
                                 where profiles.canonical_url = (select canonical_url from linkedin)\
                                 limit 1'
                db.runAnyQuery(sql_linkedin, [linkedin], function(error, linkedin_res){
                        if(err){
                                        console.log(err);
                                        callback(err,null);
                                        return;
                                }
                                console.log(rows_link[0].linkedin);

                                if(linkedin_res.rowCount == 0 ){
                                        console.log("no linkedin information...");

                                        db.runAnyQuery('select * from public.linkedin_users lu where lu.id = $1 limit 1', [linkedin], function(err, result){
                                            if(err){
                                                console.log(err);
                                                callback();
                                                return;
                                            }
                                            if(result.rowCount == 0)
                                           {
                                            console.log("no record for this user....");
                                            callback();
                                            return;
                                            }
                                        var file_linkedin_missing = './missing_linkedin_data.json'
                    
                                     callback(null, result.rows[0].canonical_url);
                                     console.log("writing missing linkedin user info for " + rows_link[0].linkedin+"...");
                                       jsonfile.writeFile(file_linkedin_missing, result.rows[0].canonical_url, {flag: 'a'}, function (err) {
                                       if(err)
                                            {console.error(err)}
                                        })


                                        })
                                       
                                     }
                                else{
                                var rows = linkedin_res.rows[0];
                                var li_json = rows.json;

                                linkedin_ind.linkedin = linkedin;


                                if(li_json.full_name)
                                {
                                        linkedin_ind.full_name = li_json.full_name;
                                }
                                else { linkedin_ind.full_name = null};

                                if(li_json.canonical_url)
                                {
                                        linkedin_ind.linkedin_url = li_json.canonical_url;
                                }
                                else{linkedin_ind.linkedin_url = null};

                                if(li_json.headline)
                                {
                                        linkedin_ind.headline = li_json.headline;
                                }
                                else{linkedin_ind.headline = null;}

                                if(li_json.industry) {
                                 linkedin_ind.industry = li_json.industry;
                                }
                                else{ linkedin_ind.industry = null; }

                                if(li_json.locality)
                                {
                                        linkedin_ind.location = li_json.locality;
                                }
                                else{ linkedin_ind.location = null;}
                
                              linkedin_ind.education = [];
                                if(li_json.education)
                                {
                                        var edu = li_json.education;
                                        for(i = 0; i <edu.length; i++){
                                                var temp_edu = {"name":null, "major": null, "summary": null};

                                                if(edu[i].name)
                                                {
                                                        temp_edu.name = edu[i].name;
                                                }

                                                if(edu[i].major)
                                                {
                                                        temp_edu.major = edu[i].major;
                                                }

                                                if(edu[i].summary)
                                                {
                                                        temp_edu.summary = edu[i].summary;
                                                }

                                                linkedin_ind.education.push(temp_edu);

                                                }

                                } 
                            
 
                                linkedin_ind.experience = [];
                                if (li_json.experience)
                                {
                                        var exp = li_json.experience;
                                        for (j =0; j< exp.length; j++)
                                        {
                                                var temp_exp = {"title": null, "description": null, "organization": null};

                                                if(exp[j].title)
                                                {
                                                        temp_exp.title = exp[j].title
                                                }

                                                if(exp[j].description)
                                                {
                                                        temp_exp.description = exp[j].description;
                                                }

                                                if(exp[j].organization)
                                                        {temp_exp.organization = exp[j].organization[0].name;}

                                                linkedin_ind.experience.push(temp_exp);
                                        }
                                }
                         

                                if(li_json.interests)
                                {
                                        linkedin_ind.interests = li_json.interests;
                                }
                                else{ linkedin_ind.interests = [];}

                                if(li_json.summary)
                                {
                                        linkedin_ind.summary = li_json.summary;
                                }
                                else{linkedin_ind.summary = null;}


                               linkedin_ind.projects = [];
                                if(li_json.projects)
                                {
                                        var proj = li_json.projects;
                                        for( i =0; i< proj.length; i++){
                                                var temp_project = {"title": null, "description": null}

                                                if(proj[i].name){
                                                        temp_project.name = proj[i].name;
                                                }

                                                if(proj[i].description)
                                                {
                                                        temp_project.description= proj[i].description;
                                                }

                                                linkedin_ind.projects.push(temp_project)

                                        }
                                        }

                                        linkedin_ind.publications= [];

                                        if(li_json.publications){
                                                var pub = li_json.publications;
                                                for(i=0; i< pub.length; i++){
                                                        var temp_publication = {"title": null, "summary": null};

                                                        if(pub[i].title)
                                                        {
                                                                temp_publication.title = pub[i].title;
                                                        }

                                                        if(pub[i].summary)
                                                        {
                                                                temp_publication.summary = pub[i].summary;
                                                        }

                                                        linkedin_ind.publications.push(temp_publication);


                                                }
                                        }

                                        if(li_json.skills)
                                        {
                                                linkedin_ind.skills= li_json.skills;
                                        }
                                        else{linkedin_ind.skills = [];}

                                        if(li_json.websites){
                                                linkedin_ind.websites = li_json.websites;
                                        }
                                        else{linkedin_ind.websites = [];}


                                var file_linkedin = './linkedin_data.json'
                    
                     callback(null, linkedin_ind);
                     console.log("writing linkedin user info for " + linkedin_ind.full_name+"...");
                     jsonfile.writeFile(file_linkedin, linkedin_ind, {flag: 'a'}, function (err) {
                        if(err)
                           {console.error(err)}
                           })
                     
                     } 
                 })

                 return;
                         }) 
               
            }, function callback(err, result){
                if(err){
                        console.log(err);
                        throw err;
                        return;

                }
            })
        }
 
                


functions.merge_linkedin()
