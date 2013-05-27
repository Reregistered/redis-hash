///////////////////////////////////////////////////
// created by: Dov Amihod
// Date : Dec 17th 2012
// purpose: This class exposes a redis backed obj. useful
// for additional resiliency in unstable environments, or across
// server restarts.
var util = require("util"),
    redis = require("redis"),
    EventEmitter = require('events').EventEmitter;

var OBJPREFIX   = '[_obj_]';

var fn_null = function(){};

function redisHash(){

  EventEmitter.call(this);
}

util.inherits(redisHash, EventEmitter);

// init the object with the connection and the hash name
redisHash.prototype.init = function(redisConnection, name, cbReady){

  var that = this;

  cbReady = cbReady || fn_null;

  this.obj = {};
  this.rc  = redisConnection;
  this.pubsub  = redis.createClient(redisConnection.port, redisConnection.host);

  this.name = name;
  this.pubsubKey = this.name + '-hash';

  var that = this;
  this.rc.hgetall(name, function(err,res){
    if (res){
      for (var itr in res){
        that.obj[itr] = res[itr];
        if(res[itr].indexOf(OBJPREFIX) === 0){
          that.obj[itr] = JSON.parse(res[itr].replace(OBJPREFIX,''));
        }
      }
    }
    cbReady(err);
  });

  // subscribe for update events.
  this.pubsub.subscribe(this.pubsubKey);
  this.on("message", function(channel, message){
    var lmessage = JSON.parse(message);
    if (lmessage.val && lmessage.val.indexOf(OBJPREFIX) === 0){
      lmessage.val = JSON.parse(lmessage.val.replace(OBJPREFIX,''));
    }
    that.emit(lmessage.type, lmessage.key, lmessage.val);
  });

};

redisHash.prototype.getVal = function(keyName, cb){
  cb = cb || fn_null;
  if (this.obj.hasOwnProperty(keyName)){
    cb(null,this.obj[keyName]);
    return;
  }
  var that = this;
  this.rc.hget([this.name,keyName], function(err,res){
    var retVal = res;
    if (res){
      if(res.indexOf(OBJPREFIX) === 0){
        retVal = JSON.parse(res.replace(OBJPREFIX,''));
      }
      that.obj[keyName] = retVal;
    }

    cb(err,retVal);
  });
};

redisHash.prototype.setVal = function(keyName, val, cb){

  cb = cb || fn_null;

  var dbVal = val;

  var that = this;
  if (val !== undefined){
    if (typeof(val) === 'object'){
      dbVal = OBJPREFIX + JSON.stringify(val);
    }

    this.rc.hset([this.name,keyName,dbVal],
      function(err,res){

        if (!err){
          that.obj[keyName] = val;

          this.pubsub.publish(this.pubsubKey, JSON.stringify({
            type:'add',
            key:keyName,
            val: dbVal}));
        }

        cb(err,res)
      }
    );

  }else{
    this.rc.hdel([this.name,keyName],
      function(err,res){
        if (!err){
          delete that.obj[keyName];

          this.pubsub.publish(this.pubsubKey, JSON.stringify({
            type:'remove',
            key:keyName}));
        }

        cb(err,res);
      }
    );
  }
};

// iterate over each element in the obj and call back
// the user with the keyname and value.
redisHash.prototype.each = function(itrCB){

  // the user may call set while we're iterating
  // in that case not sure what will happen to our
  // iterator, so copy the object, and iterate over
  // that
  var localObj = {};
  for (var itr in this.obj){
    if (this.obj.hasOwnProperty(itr)){
      localObj[itr] = this.obj[itr];
    }
  }

  for (var itr in localObj){
    if (localObj.hasOwnProperty(itr)){
      itrCB(itr, localObj[itr]);
    }
  }
};

// similar to 'each' although the caller can break
// iterating by returning true
redisHash.prototype.some = function(itrCB){

  // the user may call set while we're iterating
  // in that case not sure what will happen to our
  // iterator, so copy the object, and iterate over
  // that
  var localObj = {};
  for (var itr in this.obj){
    if (this.obj.hasOwnProperty(itr)){
      localObj[itr] = this.obj[itr];
    }
  }

  for (var itr in localObj){
    if (localObj.hasOwnProperty(itr)){
      if (itrCB(itr,localObj[itr])){
        break;
      }
    }
  }
};

redisHash.prototype.exists = function(keyName){
  if(keyName in this.obj){
    return true;
  }else{
    return false;
  }
};

exports = module.exports = redisHash;
