import core.thread;
import std.algorithm.comparison : equal;
import std.bitmanip;
import std.conv;
import std.functional;
import std.json;
import std.socket;
import std.stdio;
import std.string;

import clid;

import connection;
import dobaosll_client;
import knx;
import redis_abstractions;

enum CEMI_FROM_BAOS = "from_baos";
enum CEMI_TO_BAOS = "to_baos";

// struct for commandline params
private struct Config {
  @Parameter("config_prefix", 'c')
    @Description("Prefix for config key names. dobaosll_config_uart_device, etc.. Default: dobaosll_config_")
    string config_prefix;

  @Parameter("udp_addr", 'a')
    @Description("IP address to bind UDP socket. Default: 0.0.0.0")
    string udp_addr;

  @Parameter("port", 'p')
    @Description("UDP port. Default: 3679")
    ushort port;
}

void main() {
  writeln("hello, friend");

  auto config = parseArguments!Config();
  string config_prefix = config.config_prefix.length > 1 ? config.config_prefix: "dobaosll_config_";

  auto redisAbs = new MyRedisAbstraction();

  auto req_channel = redisAbs.getKey(config_prefix ~ "req_channel", "dobaosll_req", true);
  auto cast_channel = redisAbs.getKey(config_prefix ~ "bcast_channel", "dobaosll_cast", true);

  auto addrCfg = redisAbs.getKey(config_prefix ~ "net_udp_addr", "0.0.0.0", true);
  if (config.udp_addr.length > 1) {
    addrCfg = config.udp_addr;
    redisAbs.setKey(config_prefix ~ "net_udp_addr", addrCfg);
  }

  string portCfg = redisAbs.getKey(config_prefix ~ "net_udp_port", "3671", true);
  // if device parameter was given in commandline arguments
  if (config.port > 0) {
    portCfg = to!string(config.port);
    redisAbs.setKey(config_prefix ~ "net_udp_port", portCfg);
  }
  auto port = to!ushort(portCfg);

  // UDP socket
  auto s = new UdpSocket();
  s.blocking(false);
  auto addr = new InternetAddress(addrCfg, port);
  s.bind(addr);

  writeln("UDP socket created");

  // individual addresses for connections
  //auto indAddrCfg = getKey(config_prefix ~ "net_ind_addr", "15.15.250", true);
  auto indAddresses = redisAbs.getList(config_prefix ~ "net_ind_addr", ["15.15.240", "15.15.241"], true);
  writeln("Reserved individual addresses: ");
  writeln(indAddresses);

  auto stream_prefix = redisAbs.getKey(config_prefix ~ "stream_prefix", "dobaosll_stream_", true);
  auto stream_maxlen = redisAbs.getKey(config_prefix ~ "stream_maxlen", "100000", true);


  DobaosllClient dobaosll = new DobaosllClient();

  // ========================================== //
  KnxNetConnection[] connections;
  connections.length = indAddresses.length;
  void queue2socket(int ci) {
    // process queue. send next frames if ack received
    auto data = connections[ci].processQueue();
    if (data.length > 0) {
      connections[ci].ackReceived = false;
      connections[ci].sentReqCount += 1;
      connections[ci].lastReq = data;
      writeln("queue2socket sending data: ", data, connections[ci].addr);
      s.sendTo(data, connections[ci].addr);
    }
  }

  for (int i = 0; i < connections.length; i += 1) {
    connections[i].assignIa(indAddresses[i]);
  }
  writeln(connections);

  // available channel
  int findAvailableChannel() {
    int result = 0xff;
    for (int i = 0; i < connections.length; i += 1) {
      // if cell in array is not initialized
      if (!connections[i].active) {
        result = to!ubyte(i);
        break;
      }
    }

    return result;
  }

  void parseKnxNetMessage(ubyte[] message, Address from) {
    // example: [06 10 02 06 00 08] [00 24]
    // first, parse header
    try {
      auto headerLen = message.read!ubyte();
      if (headerLen != KNXConstants.SIZE_10) {
        writeln("wrong header length");
        return;
      }
      auto headerVer = message.read!ubyte();
      if (headerVer != KNXConstants.VERSION_10) {
        writeln("wrong version");
        // TODO: E_VERSION_NOT_SUPPORTED
        return;
      }
      auto knxService = message.read!ushort();
      auto totalLen = message.read!ushort();
      switch(knxService) {
        case KNXServices.CONNECT_REQUEST:
          writeln("ConnecRequest: ", message);
          auto hpai1len = message.read!ubyte();
          auto hpai1 = message[0..hpai1len-1];
          message = message[hpai1len-1..$];
          auto hpai2len = message.read!ubyte();
          auto hpai2 = message[0..hpai2len-1];
          message = message[hpai1len-1..$];
          auto criLen = message.read!ubyte();
          auto cri = message[0..criLen-1];
          // 4 2 0, for example
          // 4 - TUNNEL_CONNECTION
          // 2 - CRI.TUNNEL_LINK_LAYER
          // 0 - reserved
          auto connType = cri[0];

          if (connType != KNXConnTypes.TUNNEL_CONNECTION && 
              connType != KNXConnTypes.DEVICE_MGMT_CONNECTION ) {
            // send error
            auto responseFrame = connectResponseError(0x00, KNXErrorCodes.E_CONNECTION_TYPE);
            writeln("ConnectResponseError: ", responseFrame);
            sendKNXIPMessage(KNXServices.CONNECT_RESPONSE, responseFrame, s, from);
            return;
          } 
          if (connType == KNXConnTypes.TUNNEL_CONNECTION) { 
            auto knxLayer = cri[1];
            if(knxLayer != CRI.TUNNEL_LINKLAYER) {
              // check if CRI.TUNNEL_LINK_LAYER
              // go next. otherwise - ERROR unsupported E_TUNNEL_LAYER 0x29
              // send error 0x29
              auto responseFrame = connectResponseError(0x00, KNXErrorCodes.E_TUNNELING_LAYER);
              writeln("ConnectResponseError: ", responseFrame);
              sendKNXIPMessage(KNXServices.CONNECT_RESPONSE, responseFrame, s, from);
              return;
            }
          }

          // find first available cell in array
          auto chIndex = findAvailableChannel();
          if (chIndex == 0xff) {
            auto responseFrame = connectResponseError(0x00, KNXErrorCodes.E_NO_MORE_CONNECTIONS);
            writeln("ConnectResponseError: ", responseFrame);
            sendKNXIPMessage(KNXServices.CONNECT_RESPONSE, responseFrame, s, from);
            return;
          }
          // put connection into array
          auto chNumber = to!ubyte(chIndex + 1);
          connections[chIndex].active = true;
          connections[chIndex].addr = from;
          connections[chIndex].channel = chNumber;
          connections[chIndex].sequence = 0x00;
          connections[chIndex].outSequence = 0x00;
          connections[chIndex].type = connType;
          connections[chIndex].ackReceived = true;
          connections[chIndex].queue = [];
          connections[chIndex].swCon.reset();
          connections[chIndex].swCon.start();
          connections[chIndex].swAck.reset();
          ushort ia = connections[chIndex].ia;
          // send response indicating success
          auto responseFrame = connectResponseSuccess(chNumber, connType, ia);
          writeln("ConnectResponseSuccess: ", responseFrame);
          sendKNXIPMessage(KNXServices.CONNECT_RESPONSE, responseFrame, s, from);
          break;
        case KNXServices.CONNECTIONSTATE_REQUEST:
          auto chId = message.read!ubyte();
          auto reserved = message.read!ubyte();
          auto hpaiLen = message.read!ubyte();
          auto hpai = message[0..hpaiLen-1];
          message = message[hpaiLen-1..$];

          // channel value in knx is (<index in array> + 1)
          // therefore,
          bool found = connections[chId - 1].channel == chId;
          bool active = connections[chId - 1].active;

          // generate response
          if (found && active) {
            auto stateFrame = connectionStateResponse(KNXErrorCodes.E_NO_ERROR, chId);
            writeln("ConnStateResponseSuccess: ", stateFrame);
            sendKNXIPMessage(KNXServices.CONNECTIONSTATE_RESPONSE, stateFrame, s, from);
            // restart timeout watch 
            connections[chId - 1].swCon.reset();
            connections[chId - 1].swCon.start();
          } else {
            auto stateFrame = connectionStateResponse(KNXErrorCodes.E_CONNECTION_ID, chId);
            writeln("ConnStateResponseError: ", stateFrame);
            sendKNXIPMessage(KNXServices.CONNECTIONSTATE_RESPONSE, stateFrame, s, from);
          }
          break;
          // TODO: DISCONNECT_RESPONSE
        case KNXServices.DISCONNECT_RESPONSE:
          writeln("DISCONNECT_RESPONSE");
          auto chId = message.read!ubyte();
          bool found = connections[to!int(chId) - 1].channel == chId;
          bool active = connections[to!int(chId) - 1].active;
          int chIndex = -1;
          if (found) {
            chIndex = to!int(chId) - 1;
            connections[chIndex].active = false;
            connections[chIndex].channel = 0x00;
            connections[chIndex].sequence = 0x00;
            connections[chIndex].outSequence = 0x00;
            connections[chIndex].swCon.stop();
            connections[chIndex].swAck.stop();
          }
          break;
        case KNXServices.DISCONNECT_REQUEST:
          auto chId = message.read!ubyte();
          auto reserved = message.read!ubyte();
          auto hpaiLen = message.read!ubyte();
          auto hpai = message[0..hpaiLen-1];
          message = message[hpaiLen-1..$];

          // channel value in knx is (<index in array> + 1)
          // therefore,
          bool found = connections[to!int(chId) - 1].channel == chId;
          bool active = connections[to!int(chId) - 1].active;
          int chIndex = -1;
          if (found) {
            chIndex = to!int(chId) - 1;
            // disconnect and send response
            connections[chIndex].active = false;
            connections[chIndex].channel = 0x00;
            connections[chIndex].sequence = 0x00;
            connections[chIndex].outSequence = 0x00;
            connections[chIndex].swCon.stop();
            connections[chIndex].swAck.stop();

            auto disconnectFrame = disconnectResponse(KNXErrorCodes.E_NO_ERROR, chId);
            writeln("DisconnectResponse success: ...long frame...");
            sendKNXIPMessage(KNXServices.DISCONNECT_RESPONSE, disconnectFrame, s, from);
          } else {
            auto disconnectFrame = disconnectResponse(KNXErrorCodes.E_CONNECTION_ID, chId);
            writeln("DisconnectResponse error: ", disconnectFrame );
            sendKNXIPMessage(KNXServices.DISCONNECT_RESPONSE, disconnectFrame, s, from);
          }
          break;
        case KNXServices.DESCRIPTION_REQUEST:
          auto descrFrame = descriptionResponse();
          sendKNXIPMessage(KNXServices.DESCRIPTION_RESPONSE, descrFrame, s, from);
          break;
        case KNXServices.DEVICE_CONFIGURATION_ACK:
        case KNXServices.TUNNELING_ACK:
          // basically, the same. services should differ
          auto structLen = message.read!ubyte();
          auto chId = message.read!ubyte();
          // channel value in knx is (<index in array> + 1)
          // therefore,
          bool found = connections[chId - 1].channel == chId;
          bool active = connections[chId - 1].active;

          if (!found || !active) {
            // if connection is not in array or not active
            // client will resend request few times after timeout
            // then should reconnect
            return;
          }
          auto chIndex = to!int(chId) - 1;

          // 3. parse next
          // seq, 
          auto seqId = message.read!ubyte();
          auto ackSeqId = to!int(seqId);
          auto expSeqId = to!int(connections[chIndex].outSequence);
          writeln("Incoming ACK sequence id: ", ackSeqId, ", expected: ", expSeqId);
          // TODO: checkings. if not same, repeat/disconnect
          if (ackSeqId == expSeqId) {
            connections[chIndex].increaseOutSeqId();
            connections[chIndex].ackReceived = true;
            connections[chIndex].sentReqCount = 0;
            //connections[chIndex].processQueue();
            queue2socket(chIndex);
            connections[chIndex].swCon.reset();
            connections[chIndex].swCon.start();
            // stop watcher
            connections[chIndex].swAck.stop();
            connections[chIndex].swAck.reset();
          }
          break;
        case KNXServices.TUNNELING_REQUEST:
        case KNXServices.DEVICE_CONFIGURATION_REQUEST:
          auto resService = KNXServices.TUNNELING_ACK;
          // basically, the same. services should differ
          if (knxService == KNXServices.DEVICE_CONFIGURATION_REQUEST) {
            resService = KNXServices.DEVICE_CONFIGURATION_ACK;
          } else {
            // tunneling req
          }
          // 0. start parsing
          auto structLen = message.read!ubyte();
          auto chId = message.read!ubyte();

          // channel value in knx is (<index in array> + 1)
          // therefore,
          bool found = connections[chId - 1].channel == chId;
          bool active = connections[chId - 1].active;

          if (!found || !active) {
            // if connection is not in array or not active
            // client will resend request few times after timeout
            // then should reconnect
            return;
          }
          auto chIndex = to!int(chId) - 1;

          // 3. parse next
          // seq, 
          auto seqId = message.read!ubyte();

          // sequence id checkings *** debug needed
          auto clientSeqId = to!int(seqId);
          auto expectSeqId = to!int(connections[chIndex].sequence);
          writeln("Tunneling req seq id: ", clientSeqId, ", expected: ", expectSeqId);
          if (clientSeqId == expectSeqId) {
            // expected - good. ACK, process frame

            // reserved, cemi frame
            auto reserved = message.read!ubyte();
            auto cemiFrame = message[0..$];

            auto offset = 0;
            ubyte mc = cemiFrame.peek!ubyte(offset); offset += 1;
            if (mc == cEMI_MC.LDATA_REQ) {
              // get control fields from cEMI frame
              ubyte addLen = cemiFrame.peek!ubyte(offset); offset += 1;
              offset += to!int(addLen); // pass additional info
              ubyte cf1 = cemiFrame.peek!ubyte(offset); offset += 1;
              ubyte cf2 = cemiFrame.peek!ubyte(offset); offset += 1;
              ushort source = cemiFrame.peek!ushort(offset); 
              if (source == 0x0000) {
                // write ia of this connection
                // cemiFrame.write!ushort(connections[chIndex].ia, offset);
                // no. no need. should send with 0x0000
              }
              offset += 2;
              ushort dest = cemiFrame.peek!ushort(offset); offset += 2;
            }

            // acknowledge receiving request, send back to client
            auto ackFrame = ack(chId, seqId);
            sendKNXIPMessage(resService, ackFrame, s, from);

            writeln("sending to BAOS: ", cemiFrame);
            // send cemi to BAOS module
            dobaosll.sendCemi(cemiFrame);

            // store last sent frame
            connections[chIndex].lastCemiToBaos = cemiFrame.dup;
            writeln("lastCemi = ", connections[chIndex].lastCemiToBaos);

            // increase sequence number of connection
            connections[chIndex].increaseSeqId();
            // reset timeout stopwatch
            connections[chIndex].swCon.reset();
            connections[chIndex].swCon.start();
          } else if (clientSeqId == expectSeqId - 1) {
            // ACK, discard frame
            // acknowledge receiving request, send back to client
            auto ackFrame = ack(chId, seqId);
            writeln("Sending tunneling ack: ", ackFrame);
            sendKNXIPMessage(resService, ackFrame, s, from);
          } else {
            // discard
            writeln("Sequence id not expected, not one less");
          }
          break;
        default:
          writeln("Unsupported service");
          break;
      }
    } catch(Exception e) {
      writeln("Exeption processing UDP message", e);
    } catch(Error e) {
      writeln("Error processing UDP message", e);
    }
  }

  void onCemiFrame(ubyte[] cemi) {
    writeln("================= incoming ft12 cemi message =========");
    writeln(cemi);
    // Device management should support:
    // client => server
    //   M_PropRead.req
    //   M_PropWrite.req
    //   M_Reset.req
    //   M_FuncPropCommand.req
    //   M_FuncPropStateRead.req
    //   cEMI T_Data_Individual.req - dev management v2
    //   cEMI T_Data_Connected.req - dev management v2

    // In this procedure matters server => client
    //   M_PropRead.con
    //   M_PropWrite.con
    //   M_PropInfo.ind
    //   M_FuncPropStateResponse.con
    //   cEMI T_Data_Individual.ind - v2
    //   cEMI T_Data_Connected.ind - v2
    // 
    //   MPROPREAD_REQ = 0xFC,
    //   MPROPREAD_CON = 0xFB,
    //   MPROPWRITE_REQ = 0xF6,
    //   MPROPWRITE_CON = 0xF5,
    //   MPROPINFO_IND = 0xF7,
    //   MRESET_REQ = 0xF1,
    //   MRESET_IND = 0xF0

    // tunneling
    //  LDATA_REQ = 0x11,
    //  LDATA_CON = 0x2E,
    //  LDATA_IND = 0x29,

    int offset = 0;
    ubyte mc = cemi.peek!ubyte(offset); offset += 1;
    ubyte addressType = 0;
    ushort source = 0x0000;
    ushort dest = 0x0000;
    auto sourceOffset = 0;
    if (mc == cEMI_MC.LDATA_CON) {
      writeln("LData.Con frame");
      // get control fields from cEMI frame
      ubyte addLen = cemi.peek!ubyte(offset); offset += 1;
      offset += to!int(addLen); // pass additional info
      ubyte cf1 = cemi.peek!ubyte(offset); offset += 1;
      ubyte cf2 = cemi.peek!ubyte(offset); offset += 1;
      // now, get destination type: group address(1) or physical(0)
      addressType = to!ubyte((cf2 & 0x80) >> 7);
      sourceOffset = offset;
      source = cemi.peek!ushort(offset); offset += 2;
      dest = cemi.peek!ushort(offset); offset += 2;
    } else if (mc == cEMI_MC.LDATA_IND) {
      writeln("LData.Ind frame");
    } else if (mc == cEMI_MC.MPROPREAD_CON) {
      writeln("MPropRead.Con frame");
    } else if (mc == cEMI_MC.MPROPWRITE_CON) {
      writeln("MPropWrite.Con frame");
    } else if (mc == cEMI_MC.MPROPINFO_IND) {
      writeln("MPropInfo.Ind frame");
    } else {
      writeln("Unknown message code");
    }
    for (int i = 0; i < connections.length; i += 1) {
      auto conn = connections[i];
      if (!conn.active) {
        continue;
      }

      if (mc == cEMI_MC.LDATA_CON &&
          conn.type == KNXConnTypes.TUNNEL_CONNECTION) {
        auto last = connections[i].lastCemiToBaos.dup;
        writeln("comparing with: ", last);
        auto destOffset = sourceOffset + 2;
        if (last.length == cemi.length && last.length > destOffset + 1) {
          if (equal(last[destOffset..$], cemi[destOffset..$])) {
            // connection is pending LData.con message, send
            connections[i].add2queue(KNXServices.TUNNELING_REQUEST, cemi);
            queue2socket(i);
            // start timeout watcher
            connections[i].swAck.reset();
            connections[i].swAck.start();
          }
        } else if (addressType == 1) {
          // connection is not pending LData.con message
          // change it to LData.ind and send
          // BUT only if addressType indicating group address
          cemi.write!ubyte(cEMI_MC.LDATA_IND, 0);
          connections[i].add2queue(KNXServices.TUNNELING_REQUEST, cemi);
          queue2socket(i);
          // return to LDATA_CON, so, next conn[i] iterations will check correctly
          cemi.write!ubyte(cEMI_MC.LDATA_CON, 0);
          // erase info about last sent cemi data
          connections[i].lastCemiToBaos = [];
          // start timeout watcher
          connections[i].swAck.reset();
          connections[i].swAck.start();
        }
      } else if (mc == cEMI_MC.MPROPREAD_CON &&
        conn.type == KNXConnTypes.DEVICE_MGMT_CONNECTION)  {
        auto last = connections[i].lastCemiToBaos;
        writeln("last cemi:: ", connections[i].lastCemiToBaos);
        writeln("last cemi:: ", last);
        auto e = last.length;
        if (e > 0 && cemi.length > e) {
          // MPROPxx.CON is basically the same as request message
          // only data bytes added to the end of message
          if (equal(last[1..e], cemi[1..e])) {
            connections[i].add2queue(KNXServices.DEVICE_CONFIGURATION_REQUEST , cemi);
            queue2socket(i);
            connections[i].lastCemiToBaos = [];
            // start timeout watcher
            connections[i].swAck.reset();
            connections[i].swAck.start();
          }
        }
      } else if (mc == cEMI_MC.MPROPWRITE_CON
          && conn.type == KNXConnTypes.DEVICE_MGMT_CONNECTION)  {
        // send
        connections[i].add2queue(KNXServices.DEVICE_CONFIGURATION_REQUEST , cemi);
        queue2socket(i);
        // start timeout watcher
        connections[i].swAck.reset();
        connections[i].swAck.start();
      } else {
        //send unchanges to all connections
        if (mc == cEMI_MC.LDATA_IND 
            && conn.type == KNXConnTypes.TUNNEL_CONNECTION) {
          // send only group address? no.
          // LDataInd telegrams, addressed to individual device(BAOS module)
          // will be sent to all UDP connections. 
          connections[i].add2queue(KNXServices.TUNNELING_REQUEST , cemi);
          queue2socket(i);
          // start timeout watcher
          connections[i].swAck.reset();
          connections[i].swAck.start();
        } else if (mc == cEMI_MC.MPROPINFO_IND 
            && conn.type == KNXConnTypes.DEVICE_MGMT_CONNECTION)  {
          connections[i].add2queue(KNXServices.DEVICE_CONFIGURATION_REQUEST , cemi);
          queue2socket(i);
          // start timeout watcher
          connections[i].swAck.reset();
          connections[i].swAck.start();
        }
      }
    }
    writeln("================= cemi message processed =========");
  }
  dobaosll.onCemi(toDelegate(&onCemiFrame));
  while(true) {
    char[1024] buf;
    Address from;
    auto recLen = s.receiveFrom(buf[], from);
    if (recLen > 0) {
      //writeln(cast(ubyte[])buf[0..recLen], from);
      parseKnxNetMessage(cast(ubyte[])buf[0..recLen], from);
    }
    dobaosll.processMessages();
    // check connections for timeout
    for (int i = 0; i < connections.length; i += 1) {
      auto conn = connections[i];
      if (conn.active) {
        queue2socket(i);
        // also check for ack timeouts
        auto conDur = conn.swCon.peek();
        auto ackDur = conn.swAck.peek();
        auto ackTimeout = ackDur > msecs(1*1000);

        // general timeout. if no any message from client, close connection
        // for test purpose - 1minute
        auto timeout = conDur > msecs(120*1000);
        if (timeout) {
          writeln("Connection TIMEOUT: ", conn.addr);			
          connections[i].swCon.stop();
          connections[i].swCon.reset();
          // send DISCONNECT_REQUEST
          // hpai: udp(1byte), ip(4byte), port(2byte)
          ubyte[] hpai = [1, 0, 0, 0, 0, 0, 0]; 
          auto discFrame = disconnectRequest(conn.channel, hpai);
          sendKNXIPMessage(KNXServices.DISCONNECT_REQUEST , discFrame, s, conn.addr);

          // in any case 
          connections[i].active = false;
        }
        // ack timeout - if no ACK from net client for server's TUNNEL_REQ
        if (!conn.ackReceived && ackTimeout) {
          writeln("Ack not received and timeout");
          if (conn.sentReqCount == 1) {
            writeln("Sending request one more time");
            writeln("Sending: ", conn.lastReq," to:: ", conn.addr);
            // send again last frame
            s.sendTo(conn.lastReq, conn.addr);
            connections[i].sentReqCount += 1;
            // reset timeout watcher
            connections[i].swAck.reset();
          } else if (conn.sentReqCount > 1) {
            writeln("Disconnecting: ", conn.addr);
            // disconnect
            // send request at first
            ubyte[] hpai = [1, 0, 0, 0, 0, 0, 0]; 
            auto discFrame = disconnectRequest(conn.channel, hpai);
            sendKNXIPMessage(KNXServices.DISCONNECT_REQUEST , discFrame, s, conn.addr);
            // and make connection inactive
            connections[i].active = false;
            connections[i].lastReq = [];

            // stop timeout watchers
            connections[i].swCon.stop();
            connections[i].swAck.stop();
          }
        }
      }
    }
    Thread.sleep(1.msecs);
  }
}
