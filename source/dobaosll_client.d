module dobaosll_client;

import std.base64;
import std.conv;
import std.functional;
import std.json;
import std.random;
import std.stdio;
import std.string;
import std.datetime.stopwatch;

import tinyredis;
import tinyredis.subscriber;

auto rnd = Random(1);

class DobaosllClient {
  private Redis pub;
  private Subscriber sub;
  private Subscriber sub_cast;
  private string redis_host, req_channel, bcast_channel;
  private ushort redis_port;

  private bool res_received = false;
  private string res_pattern = "ddllcli_*";

  private string last_channel;
  private JSONValue response;

  private int req_timeout;

  this(string redis_host = "127.0.0.1", 
      ushort redis_port = 6379,
      string req_channel = "dobaosll_req",
      string bcast_channel = "dobaosll_cast",
      int req_timeout = 5000) {

    this.redis_host = redis_host;
    this.redis_port = redis_port;
    this.req_channel = req_channel;
    this.bcast_channel = bcast_channel;
    this.req_timeout = req_timeout;

    // init publisher
    pub = new Redis(redis_host, redis_port);
    // now handle message
    void handleMessage(string pattern, string channel, string message)
    {
      try {
        if (channel != last_channel) {
          return;
        }

        JSONValue jres = parseJSON(message);

        // check if response is object
        if (!jres.type == JSONType.object) {
          return;
        }
        // check if response has method field
        auto jmethod = ("method" in jres);
        if (jmethod is null) {
          return;
        }

        response = jres;
        res_received = true;
      } catch(Exception e) {
        //writeln("error parsing json: %s ", e.msg);
      } 
    }
    sub = new Subscriber(redis_host, redis_port);
    sub.psubscribe(res_pattern, toDelegate(&handleMessage));
  }

  public void onCemi(void delegate(ubyte[]) handler) {
    void handleMessage(string channel, string message) {
      try {
        JSONValue jres = parseJSON(message);

        // check if response is object
        if (jres.type() != JSONType.object) {
          return;
        }
        // check if response has method field
        auto jmethod = ("method" in jres);
        if (jmethod is null) {
          return;
        }
        if ((*jmethod).str != "cemi from bus") {
          return;
        }

        auto jpayload = ("payload" in jres);
        if (jpayload is null) {
          return;
        }
        try {
          auto cemi = Base64.decode(jpayload.str);
          handler(cemi);
        } catch(Exception e) {
          writeln("Exception while decoding incoming message: ", e);
        }
      } catch(Exception e) {
        //writeln("error parsing json: %s ", e.msg);
      } 
    }
    sub_cast = new Subscriber(redis_host, redis_port);
    sub_cast.subscribe(bcast_channel, toDelegate(&handleMessage));
  }

  public void processMessages() {
    sub_cast.processMessages();
  }

  public JSONValue commonRequest(string channel, string method, JSONValue payload) {
    res_received = false;
    response = null;
    last_channel = res_pattern.replace("*", to!string(uniform(0, 255, rnd)));

    JSONValue jreq = parseJSON("{}");
    jreq["method"] = method;
    jreq["payload"] = payload;
    jreq["response_channel"] = last_channel;
    pub.send("PUBLISH", channel, jreq.toJSON());

    auto sw = StopWatch(AutoStart.yes);
    auto dur = sw.peek();
    auto timeout = false;
    while(!res_received && !timeout) {
      sub.processMessages();
      dur = sw.peek();
      timeout = dur > msecs(req_timeout);
    }
    if (timeout) {
      response = parseJSON("{}");
      response["method"] = method;
      response["payload"] = "ERR_REQ_TIMEOUT";
    }

    return response;
  }

  public JSONValue commonRequest(string method, JSONValue payload) {
    return commonRequest(req_channel, method, payload);
  }
  public JSONValue sendCemi(ubyte[] cemi) {
    auto encoded = Base64.encode(cemi);
    return commonRequest("cemi to bus", JSONValue(encoded));
  }
}
