var express = require('express');

var unirest = require ('unirest');

module.exports=function()
{

  var functions={}

  // note that this has auto push set up


  functions.process = function (req, res) {
    var query=req.body.parameter;
    if(!query)
    {
      res.status(400).json({"success":false, "message":"url parameters missing for uniquest", "req":req.body});
      return;
    }
    query=query;
    unirest.get(query)
    .header({"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"})
    .end(function(response){
      console.log("data: "+response.body);

      res.status(response.statusCode).json({"success":true, "url":query,"data":response.body, "status":response.status, "headers":response.headers});
      return;
    });

    }
    return functions;
}
