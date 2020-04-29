module util;

import std.conv;
import std.string;

// to convert string "x.y.z" to 2-byte ushort value
ushort iaStr2num(string iaStr) {
  auto arr = iaStr.split(".");
  if (arr.length < 3) return 0;
  ubyte main = to!ubyte(arr[0]);
  ubyte middle = to!ubyte(arr[1]);
  ubyte group = to!ubyte(arr[2]);

  return to!ushort((((main << 4)|middle) << 8) | group);
}

string iaNum2str(ushort ia) {
  string area = to!string(ia >> 12);
  string line = to!string((ia >> 8) & 0b1111);
  string device = to!string(ia & 0xff);

  return area ~ "." ~ line ~ "." ~ device;
}

string grpNum2str(ushort grp) {
  string main = to!string(grp >> 11);
  string middle = to!string((grp >> 8) & 0b111);
  string group = to!string(grp & 0xff);

  return main ~ "/" ~ middle ~ "/" ~ group;
}
