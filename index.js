var VERSION = "1.0";
var SCAN_COUNT_SIZE = 100;
var DELAY = 100;
var async = require("async");
var redis = require("redis");
var toArray = require("stream-to-array");
var configPath;
var numSkipped = 0;
var numDeleted = 0;

require("redis-scanstreams")(redis);

// = "/Users/kciccarello/config/config";
if (process.argv.length !== 3) {
  throw new Error("Usage: node clear-passport-sessions.js <config_path>");
}

configPath = process.argv[2];

log("clear-redis-sessions v" + VERSION);
logRule();
log("Reading config from '" + configPath + "'");

var startTime = process.hrtime();
var config = require(configPath);
var REDIS_HOST = config.ENV.REDIS_HOST;
var REDIS_PORT = config.ENV.REDIS_PORT;
var REDIS_PASSWORD = config.ENV.REDIS_PASSWORD;

log("Connecting to redis: " + REDIS_HOST + ":" + REDIS_PORT);
logRule();

var client = redis.createClient(REDIS_PORT, REDIS_HOST, {
  auth_pass: REDIS_PASSWORD
});

function processKey(key, done) {
  logKey(key, "Detected logged in user...");
  
  client.get(key, function(err, reply) {
    if (err) {
      return done(err);
    }

    var result;

    try {
      result = JSON.parse(reply);
    }
    catch (err) {
      return done(err);
    }

    if (result.passport && result.passport.user && result.passport.user.userId) {
      numSkipped++;
      logKey(key, "Session contains logged-in user. Skipping...");
      return setTimeout(function() {
        done();
      }, DELAY);
    }

    logKey(key, "Deleting empty session...");

    client.del(key, function(err) {
      if (err) {
        return done(err);
      }

      numDeleted++;
      logKey(key, "Deleted.")

      setTimeout(function() {
        done();
      }, DELAY);
    });
  });
}

function log(message) {
  console.log(message);
}

function logKey(key, message) {
  log("[" + key + "] " + message);
}

function logError(message) {
var startTime = process.hrtime();  log("ERROR: " + message);
}

function logRule() {
  log(Array(11).join("-"));
}

log("Scanning redis...");

toArray(
  client.scan({ pattern: "sess:*", count: SCAN_COUNT_SIZE }),
  function(err, matches) {
    if (err) {
      log("Scan error: " + err);
    }

    log("Found " + matches.length + " match(es).");

    async.eachSeries(matches, processKey, function(err) {
      client.quit();

      if (err) {
        logError(err);
        return;
      }

      var hrend = process.hrtime(startTime);

      logRule();
      log("Execution time: " + hrend[0] + "s " + (hrend[1]/1000000).toFixed(3) + "ms");
      logRule();
      log("Matches Found: " + matches.length);
      log("Skipped: " + numSkipped);
      log("Deleted: " + numDeleted);
    });
  }
);
