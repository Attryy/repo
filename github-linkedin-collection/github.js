var express = require('express');
var fs = require('fs');
var github = require('octonode');
var unirest = require ('unirest');

module.exports=function()
{

  var functions={}

  functions.user=function(req, res, login, action)
  {
    console.log("login: "+login+" || action: "+action);
    var client = github.client(req.body.credential.github_token);
    client.requestDefaults['proxy'] = req.body.credential.proxy;
    if(!login)
    {
      res.status(400).json({"success":false, "message":"missing login"});
      return;
    }
    var ghuser   = client.user(login);
    if(!action)
    {
      res.status(400).json({"success":false, "message":"missing action"});
      return;
    }
    switch(action.trim().toLowerCase())
    {
      case 'info':
          ghuser.info(function(err, data, headers){
            res.status(200).json({"success":true, "error":err, "data":data, "headers":headers});
            return;
          });
        break;
      case 'repos':
          ghuser.repos(function(err, data, headers){
            res.status(200).json({"success":true, "error":err, "data":data, "headers":headers});
            return;
          });

        break;
    }

  }
  functions.search=function(req,res, action, query)
  {
    var client = github.client(req.body.credential.github_token);
    client.requestDefaults['proxy'] = req.body.credential.proxy;
    var ghsearch = client.search();
    if(!action)
    {
      res.status(400).json({"success":false, "message":"missing search target"});
      return;
    }
    if(!query)
    {
      res.status(400).json({"success":false, "message":"search parameters"});
      return;
    }
    console.log(JSON.stringify(query));
    switch(action.trim().toLowerCase())
    {
      case "users":
      console.log("search user");
      ghsearch.users(query, function(err, data, headers){
        res.status(200).json({"success":true, "error":err, "data":data, "headers":headers});
      });
      break;
    }
    return;
  }

  functions.repo=function(req,res, name, action)
  {
    var client = github.client(req.body.credential.github_token);
    client.requestDefaults['proxy'] = req.body.credential.proxy;
    if(!action)
    {
      res.status(400).json({"success":false, "message":"missing search target"});
      return;
    }
    if(!name)
    {
      res.status(400).json({"success":false, "message":"missing repo name"});
      return;
    }
    var ghrepo    = client.repo(name);

    switch(action.trim().toLowerCase())
    {
      case "readme":
      ghrepo.readme(function(err, data, headers){
        res.status(200).json({"success":true, "error":err, "data":data, "headers":headers});
        return;
      });
      break;
      case "commits":
      ghrepo.commits(function(err, data, headers){
        res.status(200).json({"success":true, "error":err, "data":data, "headers":headers});
        return;
      });
      break;
    }
    return;
  }

  functions.org=function(req,res, name, action, query)
  {
    var client = github.client(req.body.credential.github_token);
    client.requestDefaults['proxy'] = req.body.credential.proxy;
    if(!action)
    {
      res.status(400).json({"success":false, "message":"missing search target"});
      return;
    }
    if(!name)
    {
      res.status(400).json({"success":false, "message":"missing org name"});
      return;
    }
    if(!query)
    {
      res.status(400).json({"success":false, "message":"missing org query"});
      return;
    }
    var ghorg = client.org(name);

    switch(action.trim().toLowerCase())
    {
      case "members":
      ghorg.members(query,function(err, data, headers){
        res.status(200).json({"success":true, "error":err, "data":data, "headers":headers});
        return;
      });
      break;
    }
    return;
  }

  functions.process = function (req, res) {
      switch(req.params.sub)
      {
        case "user":
          functions.user(req, res, req.params.subsub, req.params.subsubsuburl);
          break;
        case "search":
        console.log("p: "+req.body.parameter)
          functions.search(req,res, req.params.subsub, JSON.parse(req.body.parameter));
          break;
        case "repo":
          functions.repo(req, res, req.body.parameter,req.params.subsub );
          break;
        case "org":
          functions.org(req, res, req.params.subsub, req.params.subsubsuburl, JSON.parse(req.body.parameter));
          break;
      }
    }

    return functions;
}

functions.process();
