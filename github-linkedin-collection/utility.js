const db=require('./db');

module.exports.giveMeSetOfCredentials =function(callback) {
    db.runAnyQuery("with pre as (select * from credentials where invalid_github_token=false order by random() limit 1) select b.ip, b.secret, p.* from bots b\
                    inner join pre p on true\
                    order by random() limit 1", null, function(e, r){
      if(e)
      {
        callback(null, e);
        return;
      }
      if(r.rows.length==0)
      {
        callback(null, "no bots available");
        return;
      }

      ////"http://jchen04:hNf0K6dw@172.241.143.167:29842"
      var json={
                          "github_token":r.rows[0].github_token,
                          "meetup_token":r.rows[0].meetup_token,
                          "twitter_handle":r.rows[0].twitter_handle,
                          "twitter_id":r.rows[0].twitter_id,
                          "twitter_consumer_key":r.rows[0].twitter_consumer_key,
                          "twitter_consumer_secret":r.rows[0].twitter_consumer_secret,
                          "twitter_access_token":r.rows[0].twitter_access_token,
                          "twitter_access_secret":r.rows[0].twitter_access_secret,
                          'proxy':r.rows[0].proxy,
                          "secret":r.rows[0].secret,
                          "ip":r.rows[0].ip
                        };
    callback(json);
  });
};

module.exports.replaceInvalidGithubToken=function(token, markOldAsInvalid, callback)
{

  var sql;

  if(markOldAsInvalid)
  {
    sql="with pre as (update credentials set invalid_github_token=true where github_token=$1)\
              select proxy, github_token from credentials where invalid_github_token=false and github_token<>$1 order by random() limit 2"
  }
  else {
    sql="select proxy, github_token from credentials where invalid_github_token=false and github_token<>$1 order by random() limit 2"
  }

  const args=[token]
  db.runAnyQuery(sql, args, function(e, r){
    if(e)
    {
      callback(null, e);
      return
    }
    else if(r.rows.length==0)
    {

      callback(null, "no bots available");
      return;
    }
    else {

      callback({"token":r.rows[0].github_token, "proxy":r.rows[0].proxy});
      return;
    }
  });
}
