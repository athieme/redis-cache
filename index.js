/**
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 */

'use strict';

var Promise = require('bluebird');
var util = require('util');
var moment = require('moment');
var redis = require('redis');
var _ = require('lodash');

module.exports = function (config) {

  var client = Promise.promisifyAll(redis.createClient(
    config.port,
    config.host,
    config.options
  ));
  client.on('error', function (err) {
    console.log('redis err', err);
  });

  var _expiration_key = function (key) {
    return util.format('%s:expiration', key);
  };

  var _get = function (key) {
    return client.getAsync(key);
  };
  var _set = function (key, ttl, response) {
    return client.setexAsync(key, ttl, response).return(response);
  };

  var _hmget = function (hash, key) {
    // get the expiration key
    var expire_key = _expiration_key(key);
    return client.hmgetAsync(hash, key, expire_key)
      .then(function (result) {
        // if has expiration key and not expired
        var now = moment().unix();
        if (result && result[1] && now < result[1]) {
          // then get keyed hash value
          console.log('_hget NOT expired now', now, ', expiration', result[1]);
          //return client.hgetAsync(hash, key);
          return result[0];
        } else {
          console.log('_hget expired now', now, ', expiration', result);
          // remove keyed hash value
          return null;
        }
      });
  };

  var _hmset = function (hash, key, ttl, value) {
    // set the keyed hash value
    // since hash sets don't support ttl
    // create another value w expiration
    var expire_key = _expiration_key(key);
    var expiration = moment().utc().add('seconds', ttl).unix();
    return new Promise(function (resolve) {
      client.multi()
        .hmset(hash, key, value, expire_key, expiration)
        .expire(hash, ttl)
        .exec(function (err, replies) {
          return resolve(value);
        });
    });
  };

  return {
    get : function (key) {
      return _get(key);
    },
    put : function (key, value, ttl) {
      return _set(key, ttl, value);
    },
    cached : function (key, ttl, handler) {
      return _get(key)
        .then(function (result) {
          if (_.isNull(result)) {
            console.log('cached key %s, response %j', key, JSON.parse(result), {});
            return result;
          } else {
            return Promise.reject(util.format('uncached key %s', key));
          }
        })
        .catch(function (err) {
          console.log('cached err', err);
          return handler().then(function (response) {
            var value = JSON.stringify(response);
            console.log('caching key %s, response %s', key, value);
            return _set(key, ttl, value).then(function () {
              return response;
            });
          });
        });
    },
    invalidate : function (key) {
      return client.delAsync(key);
    },
    forBucket : function (hash) {
      return {
        get : function (key) {
          return _hmget(hash, key);
        },
        put : function (key, value, ttl) {
          return _hmset(hash, key, ttl, value);
        },
        cached : function (key, ttl, handler) {
          return _hmget(hash, key)
            .then(function (result) {
              if (result) {
                console.log('cached hash %s, key %s, response %j', hash, key, JSON.parse(result), {});
                return result;
              } else {
                return Promise.reject(util.format('uncached hash %s, key %s', hash, key));
              }
            })
            .catch(function (err) {
              console.log('forBucket cached err', err);
              return handler().then(function (response) {
                var value = JSON.stringify(response);
                console.log('caching key %s, response %s', key, value);
                _hmset(hash, key, ttl, value);
                return response;
              });
            });
        },
        invalidate : function (key) {
          var expire_key = _expiration_key(key);
          return client.hdelAsync(hash, key, expire_key);
        },
        invalidateAll : function () {
          return client.delAsync(hash);
        }
      };
    }
  };
};
