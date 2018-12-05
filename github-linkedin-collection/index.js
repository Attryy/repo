var express = require('express');
var unirest = require ('unirest');
var router = express.Router();
var Github = require('./github');
var GithubV4 = require('./githubv4');
var Meetup = require('./meetup');
var Twitter = require('./twitter');
var Linkedin = require('./linkedin');


var deepcopy = require("deepcopy");
const db=require('./db');
const utility = require('./utility');

router.get('/', function(req, res, next) {
  res.json({"message":"OK"})
});

router.get('/list', function(req, res, next) {

  if(!req.query.secret || req.query.secret!="taK7bflBykd1KQG3mmkP")
  {
    res.json({"error":"Invalid secret key"})
    return;
  }
  else {
    db.runAnyQuery("select array_agg(ip) as ips from bots", null, function(e, r){
      if(e)
      {
        console.log(e);
        res.json({"error":("Internal db error...."+e)});
        return;
      }
      else if(!r.rows || r.rows.length==0)
        {
          res.json({"error":"Empty databots table..."});
          return;
        }
      else {
        res.json({"servers":r.rows[0].ips});
        return;
      }


    })
  }
});

router.post('/private/:url?/:sub?/:subsub?/:subsubsuburl?/', function(req, res, next) {

    utility.giveMeSetOfCredentials(function(credentials, e){

      if(e)
      {
        console.log(e)
        res.status(400).json({"success":false, "message":"internal error --> acquiring credentials failed"});
        return;
      }
      var url = req.originalUrl;
      url=url.replace('private','slave');
      url=url.replace(/secret=\w+\b/,('secret='+credentials.secret));
      url="http://localhost:5000"+url;
      console.log(url);
      var json=deepcopy(req.body);
      json["credential"]=credentials;
/*      json["credential"]={
                          "github_token":"8eb9ef8e9cf747cdb8f68cc85a589c2a030ef71d",
                          "meetup_token":"503db43391156c2a3f115d1a524e",
                          "twitter_handle":"Paul11Maxfield",
                          "twitter_id":"788315873442340870",
                          "twitter_consumer_key":"Wj6JHrwr1t04kJT5UTNRJ3SFx",
                          "twitter_consumer_secret":"GSt9QHjMpfGW53OCDSLdukxCaUmLTm4Smtzqx85c1jZgnSR6k4",
                          "twitter_access_token":"788315873442340870-SGs74L2Vv3WZ8lJQpP9asJuEkYZ8TAw",
                          "twitter_access_secret":"IxL1294R5lV0L8vPvE4Z8WSK3wPPr0WBYvY3bfFfb5KrI",
                          'proxy':"http://jchen04:hNf0K6dw@172.241.142.158:29842"
                        };
                        */
      unirest.post(url)
      .headers({'Accept': 'application/json', 'Content-Type': 'application/json'})
      .send(json)
      .end(function(response){
        if (response && response.statusCode) {
          res.status(response.statusCode).json(response.body);
          return;
        }
        else {
          res.status(400).json({});
          return;
        }

    });

    })

});


router.post('/master/:url?/:sub?/:subsub?/:subsubsuburl?/', function(req, res, next) {

        utility.giveMeSetOfCredentials(function(credentials, e){
          if(e)
          {
            console.log(e)
            res.status(400).json({"success":false, "message":"internal error --> acquiring credentials failed"});
            return;
          }
          var url = req.originalUrl;
          url=url.replace('master','slave');
          url=url.replace(/secret=\w+\b/,('secret='+credentials.secret));
          console.log("target ip:", credentials.ip);

          url="http://"+credentials.ip+url;

          console.log(url);

          var json=deepcopy(req.body);
          ////"http://jchen04:hNf0K6dw@172.241.143.167:29842"
          json["credential"]=credentials;
          unirest.post(url)
          .headers({'Accept': 'application/json', 'Content-Type': 'application/json'})
          .send(json)
          .end(function(response){
            if (response && response.statusCode) {
              res.status(response.statusCode).json(response.body);
              return;
            }
            else {
              res.status(400).json({});
              return;
            }
        });
      });
});

router.post('/slave/:url?/:sub?/:subsub?/:subsubsuburl?/', function(req, res, next) {


    switch(req.params.url.trim().toLowerCase())
    {
      case "github":
        if(!req.params.url || !req.params.sub)
        {
          res.status(400).json({"success":false, "message":"Missing URL structure"});
          return;
        }

        if( !req.body.credential.github_token)
        {
          res.status(400).json({"success":false, "message":"this machine is missing github api key"});
          return;
        }
        var github=new Github();
        github.process(req, res);
      break;
      case "githubv4":
        if(!req.params.url || !req.params.sub)
        {
          res.status(400).json({"success":false, "message":"Missing URL structure"});
          return;
        }

        if( !req.body.credential.github_token)
        {
          res.status(400).json({"success":false, "message":"this machine is missing github api key"});
          return;
        }
        var githubv4=new GithubV4();
        githubv4.process(req, res);
      break;
      case "meetup":
        if( !req.body.credential.meetup_token)
        {
          res.status(400).json({"success":false, "message":"this machine is missing meetup api key"});
          return;
        }
        var meetup=new Meetup();
        meetup.process(req, res);
      break;
      case "twitter":
      console.log("twitter requested.....", req.body);
        if( !req.body.get || !req.body.credential.twitter_consumer_key || !req.body.credential.twitter_consumer_secret || !req.body.credential.twitter_access_token || !req.body.credential.twitter_access_secret)
        {
          res.status(400).json({"success":false, "message":"this machine is missing Twitter key or parameters"});
          return;
        }
        var twitter=new Twitter();
        twitter.process(req, res);
      break;
      case "test":
      unirest.post("http://localhost/slave/linkedin?secret=odugTRNClaVRZkAWGUno")
      .send('parameter='+"http://www.linkedin.com/in/williamhgates")
      .end(function (response_from_server) {
        res.json({"message":"sent"});
      });
      break;
    }


});

module.exports = router;
