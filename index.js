///////////////////////////////////////////////////
// created by: Dov Amihod
// Date : Dec 17th 2012
// purpose: This class exposes a redis backed obj. useful
// for additional resiliency in unstable environments, or across
// server restarts.
var util = require("util"),
    guid = require("node-guid"),
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

  cbReady = cbReady || fn_null;

  this.obj = {};
  this.rc  = redisConnection;

  this.name = name;

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

  this.initPubSub();

};

redisHash.prototype.initPubSub = function(){

  var that = this;

  this.instanceId = guid.new();
  this.pubsub = redis.createClient(this.rc.port, this.rc.host);
  this.pubsubKey = this.name + '-hash';

  // subscribe for update events.
  this.pubsub.subscribe(this.pubsubKey);

  // setup the pub sub message handler
  this.pubsub.on("message", function(channel, message){

    // set up the message object.
    var lmessage = JSON.parse(message);

    // if we've sent the message, ignore it
    if (lmessage.sender === that.instanceId){
      return;
    }

    if (lmessage.val && String(lmessage.val).indexOf(OBJPREFIX) === 0){
      lmessage.val = JSON.parse(lmessage.val.replace(OBJPREFIX,''));
    }

    switch (lmessage.type){
      case 'reset':
        that.obj = {};
        break;
      case 'add':
        that.obj[lmessage.key] = lmessage.val;
        break;
      case 'remove':
        delete that.obj[lmessage.key];
        break;
      case 'default':
        // unhandled message
        break;
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

          that.rc.publish(that.pubsubKey, JSON.stringify({
            sender: that.instanceId,
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
          that.rc.publish(that.pubsubKey, JSON.stringify({
            sender: that.instanceId,
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
  return (this.obj[keyName] != undefined);
};

redisHash.prototype.reset = function(){
  var that = this;
  this.rc.del([this.name],
    function(err,res){
      if (!err){
        this.obj = {};
        that.rc.publish(that.pubsubKey, JSON.stringify({
          sender: that.instanceId,
          type:'reset'}));
      }
    });
};

exports = module.exports = redisHash;
