var TwitterBot = require('./TwitterBot.js')
var jsonfile = require('jsonfile')

var bot = new TwitterBot();
var params =  {"q":'Hannah21861641', "page":1, "count": 20, 'include_entities':true} // search parameters: only take 500 search results available

//bot.getContent("users/search",params, function(err, json, res){
 bot.getContent("users/search", params, function(err, json, res){
  var patt = new RegExp(/[^\x00-\x7F]+/);
  console.log(patt.test('S@JDn'));
  if (err){
  	 console.log(err)
     return;
   }
  if(Object.keys(json).length ==0){ console.log('NO results returned')}
  else{
  	console.log(json.length)
    jsonfile.writeFile('./testSearch.json',json,{flag: 'a'}, function (write_err){
      	if(write_err){
   		   console.log(write_err);
   		   throw write_err;
   		   return;
   	    }
    })

   }
  return;
})
