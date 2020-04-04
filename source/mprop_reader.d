import core.thread;
import std.algorithm: equal;
import std.bitmanip;
import std.conv;
import std.functional;

import dobaosll_client;
import knx;
import redis_abstractions;

class MPropReader {
  private DobaosllClient dobaosll;
  // global variables
  private ubyte[] lastRequest;
  private ubyte[] responseData;
  private bool resolved = false;


  this() {
    dobaosll = new DobaosllClient();
    void onCemiFrame(ubyte[] cemi) {
      int offset = 0;
      ubyte mc = cemi.peek!ubyte(offset); offset += 1;
      if (mc == cEMI_MC.MPROPREAD_CON) {
        auto e = lastRequest.length;
        if (e > 0 && cemi.length > e) {
          // MPROPxx.CON is basically the same as request message
          // only data bytes added to the end of message
          if (equal(lastRequest[1..e], cemi[1..e])) {
            resolved = true;
            responseData = cemi[e..$].dup;
          }
        }
      }
    }
    dobaosll.onCemi(toDelegate(&onCemiFrame));

  }
  public ubyte[] read(ubyte id, int num = 1, ushort si = 0x0001) {
    ubyte[] request;
    request.length = 7;
    request.write!ubyte(cEMI_MC.MPROPREAD_REQ, 0);
    request.write!ushort(0, 1); // interface object type
    request.write!ubyte(1, 3); // object instance
    request.write!ubyte(id, 4); // property id
    ushort noeSix = to!ushort(num << 12 | (si & 0b111111111111));
    request.write!ushort(noeSix, 5);

    lastRequest = request.dup;
    dobaosll.sendCemi(request);
    while (!resolved) {
      dobaosll.processMessages();
      Thread.sleep(1.msecs);
    }
    auto result = responseData.dup;
    lastRequest = [];
    responseData = [];
    resolved = false;

    return result;
  }
}
