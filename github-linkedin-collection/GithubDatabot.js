var async = require('async');
var unirest = require('unirest');
var parse = require('parse-link-header');
var sleep = require('sleep');

var master_url=<master_url>

function GithubDatabot() {

};



GithubDatabot.prototype.userV4 = function (user_login) {
  var login=user_login;
  var counter = 0;
  return {

    info:function(callback)
    {
      counter ++;
      var self = this;
      var url=master_url+'githubv4/user/'+login+'/info?secret=<secretToken>';
      console.log(url);
      unirest.post(url)
      .send()
      .end(function (response) {
        if(response.statusCode!=200)
        {
          if((response.error && (response.error.code == "ETIMEDOUT"||response.error.code == "ENOTFOUND" ||response.error.code=="ECONNRESET" || response.error.code=="ECONNREFUSED"))|| response.statusCode==504 || response.statusCode == 502 )
          {
            self.info(callback);
            return;
          }
          if (response.statusCode == 403)
          {
            console.log("403 error,about to sleep...");
            //sleep.sleep(60);
            self.info(callback);
          }
          else {
            console.log(JSON.stringify(response));
            throw response;
            return;
          }

        }
        if(!response.body.error)
        {
          if( response.body.data == null){
            if(counter <=4){
              console.log("No data, retrying...");
              self.info(callback); 
              return;
            }
            else{
              callback(response.body.error, response.body.data, response.body.headers);
               return;
            }
        
          }
          else{
            callback(response.body.error, response.body.data, response.body.headers);
            return;
          }
        }
        else {
          if (response.body.error=="You have triggered an abuse detection mechanism. Please wait a few minutes before you try again.")
          {
            console.log("about to sleep...")
            sleep.sleep(70);
            self.info(callback);
          }
          else{
            console.log( JSON.stringify(response.body.error));
             throw response.body;
            return;
            }
        }
      });
    }

  };
};



GithubDatabot.prototype.user = function (user_login) {
  var login=user_login;
  return {

    info:function(callback)
    {
      var self = this;
      var url=master_url+'github/user/'+login+'/info?secret=<secretToken>';
      console.log(url);
      unirest.post(url)
      .send()
      .end(function (response) {
        if(response.statusCode!=200)
        {
          console.log(JSON.stringify(response));
          if(response.error && (response.error.code=="ECONNRESET" || response.error.code=="ECONNREFUSED"))
          {
            self.info(callback);
            return;
          }
          else {
            throw response;
            return;
          }

        }
        if(response.body.success)
        {
          if(response.body.error && response.body.error.statusCode==403)
          {

            self.info(callback);
            return;
          }
          else {
            callback(response.body.error, response.body.data, response.body.headers);
            return;
          }

        }
        else {
          console.log(JSON.stringify(response.body));
          throw response.body;
          return;
        }
      });
    },
    repos:function(callback)
    {
       var self = this;
        var url=master_url+'github/user/'+login+'/repos?secret=<secretToken>';
        console.log(url);
        unirest.post(url)
        .send()
        .end(function (response) {
          if(response.statusCode!=200)
          {
            console.log(JSON.stringify(response));
            if(response.error && (response.error.code=="ECONNRESET"|| response.error.code=="ECONNREFUSED"))
            {
              console.log("error from server:", response.error);
              self.repos(callback);
              return;
            }
            else {
              throw response;
              return;
            }

          }

          if(response.body.success)
          {
            if(response.body.error &&( response.body.error.statusCode==403 || (response.body.error.code=="ECONNRESET"|| response.body.error.code=="ECONNREFUSED")))
            {
              self.repos(callback);
              return;
            }
            else {
              callback(response.body.error, response.body.data, response.body.headers);
              return;
            }
          }
          else {
            console.log(JSON.stringify(response));

            throw response;
            return;
          }
        });
    }


  };
};





GithubDatabot.prototype.repo = function (repo_name) {
  var repo=repo_name;
  return {
    readme:function(callback)
    {
      var self = this;
      var url=master_url+'github/repo/readme?secret=<secretToken>';
      console.log(url);
        unirest.post(url)
        .send('parameter='+repo)
        .end(function (response) {
          if(response.statusCode!=200)
          {
            console.log(JSON.stringify(response));
            if(response.error && ( response.error.code=="ECONNRESET" || response.error.code=="ECONNREFUSED")|| response.statusCode == 502 )
            {
              console.log("error from server:", response.error);
              self.readme(callback);
              return;
            }
            else {
              throw response;
              return;
            }

          }
          if(response.body.success)
          {
            if(response.body.error && response.body.error.statusCode==403)
            {
              if( response.body.error.headers &&  response.body.error.headers["x-ratelimit-remaining"] =="0") // an API rate limit error, get another bot to do it;
              {
                console.log("rate limit exceeded, finding another one");
                self.readme(callback);
              }
              else {
                  callback(response.body.error, response.body.data, response.body.headers);
              }
              return;
            }
            else {
              callback(response.body.error, response.body.data, response.body.headers);
              return;
            }
          }
          else {
            console.log(JSON.stringify(response.body));
            throw response.body;
            return;
          }
        });
    },
    commits:function(callback)
    {
      var self = this;
      var url=master_url+'github/repo/commits?secret=<secretToken>';
      console.log(url);
        unirest.post(url)
        .send('parameter='+repo)
        .end(function (response) {
          if(response.statusCode!=200)
          {
            console.log(JSON.stringify(response));
            if(response.error && (response.error.code=="ECONNRESET" || response.error.code=="ECONNREFUSED"))
            {
              self.commits(callback);
              return;
            }
            else {
              throw response;
              return;
            }

          }
          if(response.body.success)
          {
            if(response.body.error && response.body.error.statusCode==403)
            {
              self.commits(callback);
              return;
            }
            else {
              callback(response.body.error, response.body.data, response.body.headers);
              return;
            }

          }
          else {
            console.log(JSON.stringify(response.body));
            throw response.body;
            return;
          }
        });
    }

  };
};





GithubDatabot.prototype.org = function (repo_login) {
  var login=repo_login;
  return {
    members:function(parameter, callback)
    {
      var self = this;
      var url=master_url+'github/org/'+login+'/members?secret=<secretToken>';
      console.log(url);

        unirest.post(url)
        .send('parameter='+JSON.stringify(parameter))
        .end(function (response) {
          if(response.statusCode!=200)
          {
            console.log(JSON.stringify(response));
            if(response.error && (response.error.code=="ECONNRESET" || response.error.code=="ECONNREFUSED"))
            {
              self.members(parameter, callback);
              return;
            }
            else {
              throw response;
              return;
            }

          }
          if(response.body.success)
          {
            if(response.body.error && response.body.error.statusCode==403)
            {
              self.members(parameter, callback);
              return;
            }
            else {
              callback(response.body.error, response.body.data, response.body.headers);
              return;
            }

          }
          else {
            console.log(JSON.stringify(response.body));
            throw response.body;
            return;
          }
        });
    }
  };
};


// A function to return the graphQL query results on users given a specified page number and number of users per page
// the results are sorted by the time of account creation in an ascending order
GithubDatabot.prototype.searchNoPagination = function () {
 return {
   users:function(query, per_page,page, callback)
    {
      var self = this;
      var parameters={
        q: query,
        sort: 'created',
        order: 'asc'
      } 
      if(per_page)
      {
        parameters['per_page'] = per_page;
      }
      if(page)
      {
        parameters['page']= page;
      }
      var url=master_url+'github/search/users?secret=<secretToken>';
      console.log(url);
        unirest.post(url)
        .send('parameter='+JSON.stringify(parameters))
        .end(function (response) {
          if(response.statusCode!=200)
          {
            console.log(JSON.stringify(response));
            if(response.error && (response.error.code=="ECONNRESET" || response.error.code=="ECONNREFUSED") || response.statusCode == 502||response.statusCode==504)
            {
              self.users(parameters, callback);
              return;
            }
            else {
              throw response;
              return;
            }

          }
          if(response.body.success)
          {
            if(response.body.error && response.body.error.statusCode==403)
            {
              self.users(parameters, callback);
              return;
            }
            else {
              callback(response.body.error, response.body.data, response.body.headers);
              return;
            }

          }
          else {
            console.log(JSON.stringify(response.body));
            throw response.body;
            return;
          }
        });
    }
  };
};

// this function is designed to return search results (github api V3) in a more flexible manner that is a generalization of the seachNoPagination function. 
// When a search query returns too many results, we use paginations to collect all of them sequentially. 
// We specify the maximum number of results to return per page to be 100, so once the total number of results exceeds per_page_max = 100, pagination is needed. 
// However, if the total number of results exceeds 500, we stop collecting results for this particular query by calling back null response. 
// When the total number of results is between 100 and 500, we use a loop to call searchNoPagination iteratively until we reach the last page and use a Promise to wait until all results have been collected into fullresult. 

GithubDatabot.prototype.search = function () {
  return {
    users:function(query, per_page, page, callback)
    {
      var self = this;
      var per_page_max = 100;
      var parameters={
        q: query,
        sort: 'created',
        order: 'asc'
      } 
      if(per_page)
      {
        parameters['per_page'] = per_page;
      }
      if(page)
      {
        parameters['page']= page;
      }

      var url=master_url+'github/search/users?secret=<secretToken>';
      console.log(url);
        unirest.post(url)
        .send('parameter='+JSON.stringify(parameters))
        .end(function (response) {
          if(response.statusCode!=200)
          {
            // console.log(JSON.stringify(response));
            if(response.error && (response.error.code=="ECONNRESET" || response.error.code=="ECONNREFUSED" || response.error.code =="ETIMEDOUT") || response.statusCode == 502 ||response.statusCode ==504 )
            {
              self.users(query, per_page, page, callback);
              return;
            }
            else {
              console.log(response);
              throw response;
              return;
            }

          }
          if(response.body.success)
          {
            if(response.body.error && response.body.error.statusCode==403)
            {
              self.users(query, per_page, page, callback);
              return;
            }
            else {
              if(response.body.data.total_count > per_page_max && response.body.data.total_count < 500){
                console.log("pagination...");
                var parsed = parse(response.body.headers.link)
                var page_last = parsed.last.page;

                var page_id = [];
                for(var i = 1; i<= page_last; i++){
                  page_id.push(i);
                }
                console.log(page_id);
                // Re-write it using async... until... +++
                var users = page_id.map(function(id){ 
                  return new Promise(function(resolve, reject){
                    var Ssearch = GithubDatabot.prototype.searchNoPagination();
                  return  Ssearch.users(query, per_page= 100, page = id, function(err, data, headers){
                    if (err){
                      throw err;
                      console.log(err);
                      reject(data);
                      return;
                    }
                    if(headers['x-ratelimit-remaining']==0)
                    {
                       console.log("sleeping...")
                       sleep.sleep(60);
                     }
                     else {
                      var response = {"error": err, "data": data, "headers":headers};
                      resolve(response);
                     }
                  });
                });
                });
                 
                Promise.all(users).then(function(result){
                  var full_result = result.map(function(user){
                     return user;
                  });
                  // return an array of each individual call
                   //console.log(full_result);
                   var count = 0;
                   async.whilst(function(){return count < page_last},
                    function(cb){
                    console.log("looping over pages..."+"page:"+(count+1));
                    callback(full_result[count].error, full_result[count].data, full_result[count].headers);
                    count = count+1;
                    cb();
                  

                 },
                  function(err){
                   console.log(err);
                    return;
                  });
              }).catch(function(error){
                console.log(error);
                throw error;
                return;
              });
            }
             else{ 
                  if(response.body.data.total_count>=500){
                    response.body.error = "too many potential matches...";
                    response.body.data = null;
                  }
                 callback(response.body.error, response.body.data, response.body.headers);
                 return;
              }
             
            }

          }
          else {
            console.log(JSON.stringify(response.body));
            throw response.body;
            return;
          }
        });
    }
  };
};



module.exports = GithubDatabot;
