@0xcc6761a82bcaad95;

# The values of the records of the benchmarks, without the id: kvutil keeps it in the key.
#
# Cap'n Proto has no optional text: a Text that is not set reads as the empty string. The
# optional strings of the Go records (Order.Note, Book.Subtitle) are told apart by a Bool next
# to them: noteSet and subtitleSet (not hasNote: the Go generator names the accessor of a Text
# HasNote already, and the two clash). Time is Unix nanoseconds.

using Go = import "/go.capnp";
$Go.package("capgen");
$Go.import("github.com/ehsanranjbar/kvutil/bench/formats/capgen");

struct Order {
  code @0 :Text;
  customer @1 :Text;
  name @2 :Text;
  status @3 :Text;
  total @4 :Int64;
  qty @5 :Int32;
  at @6 :Int64;
  noteSet @7 :Bool;
  note @8 :Text;
  tags @9 :List(Text);
  payload @10 :Data;
}

struct Book {
  code @0 :Text;
  author @1 :Text;
  year @2 :Int64;
  price @3 :Int64;
  inPrint @4 :Bool;
  tags @5 :List(Text);
  title @6 :Text;
  subtitleSet @7 :Bool;
  subtitle @8 :Text;
  score @9 :Int64;
  payload @10 :Data;
}

struct Doc {
  lang @0 :Text;
  year @1 :Int64;
  embedding @2 :List(Float32);
}
