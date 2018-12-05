var async = require('async');
var unirest = require('unirest');

module.exports=function(res)
{
  var functions={};

  functions.getContent=function(getTxt, params, callback){
    //console.log("twitter bot params:", params);
     var self = this;
     var url=process.env.isLocal?'<serverForBot>';
     unirest.post(url)
     .headers({ 'Content-Type': 'application/json'})
     .send({"get":getTxt, "parameters":params}) // getTxt can be 'users/search' fro user search api; eg: parameters = {"q": 'Tom Lee', "page": 1, "count":2}
     .end(function (response){
           if(response.statusCode!=200){
                 console.log(JSON.stringify(response));
                 if(response.error && (response.error.code=="ECONNRESET" || response.error.code=="ECONNREFUSED")){
                       console.log("ECONNRESET");
                       self.getContent(getTxt, params, callback);
                       return;
                  }
                 else{
                       callback("invalid error", response, response);
                       return;
                  }
           }
           if(response.body.success){
                if(response.body.error && response.body.error.length>0 && (response.body.error[0].code==215 || response.body.error[0].code==32||err[0].code == 131|| err[0].code == 41)){
                     console.log("authentication wrong / internal error ...trying again")
                     self.getContent(getTxt, params, callback);
                     return;
                }
                else{
                     callback(response.body.error, response.body.json, response.body.response);
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
  return functions;
}
