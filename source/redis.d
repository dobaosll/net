module redis_abstractions;

import std.conv;
import std.json;
import std.stdio;

import tinyredis;
import tinyredis.subscriber;

class MyRedisAbstraction {
  private Redis redis;
  this() {
    redis = new Redis("127.0.0.1", cast(ushort)6379);
  }
  public string setKey(string key, string value) {
    auto cmd  = "SET " ~ key ~ " " ~ value;
    return redis.send(cmd).toString();
  }
  public string getKey(string key) {
    return redis.send("GET " ~ key).toString();
  }
  // get key with default value
  public string getKey(string key, string default_value, bool set_if_null = false) {
    auto keyValue = redis.send("GET " ~ key).toString();

    if (keyValue.length > 0) {
      return keyValue;
    } else if (set_if_null) {
      setKey(key, default_value);
      return default_value;
    } else {
      return default_value;
    }
  }
  public string[] getList(string key) {
    string[] result;
    auto response = redis.send("LRANGE", key, "0", "-1");
    foreach(r; response) {
      result ~= to!string(r);
    }
    return result;
  }
  public string[] getList(string key, string[] defaultList, bool set_if_null = false) {
    string[] result;
    auto response = redis.send("LRANGE", key, "0", "-1");
    foreach(r; response) {
      result ~= to!string(r);
    }
    if (result.length == 0 && set_if_null) {
      // default values 
      foreach(df; defaultList) {
        redis.send("RPUSH", key, df);
      }
      return defaultList;
    } else if (result.length == 0) {
      return defaultList;
    }
    return result;
  }
  public void addToStream(string key_prefix, string maxlen, JSONValue data) {
    auto command = "XADD ";
    command ~= key_prefix ~ data.array[0].str ~ " ";
    command ~= "MAXLEN ~ " ~ to!string(maxlen) ~ " ";
    command ~= "* "; // id
    command ~= "payload " ~ data.array[1].toJSON() ~ " ";
    redis.send(command);
  }
}
