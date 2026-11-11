# bench/formats

A throwaway module that measures how the value formats compare for the stored records of kvutil,
to choose between FlatBuffers and Cap'n Proto: msgpack (the default of kvutil), the binary codec
written by hand (Order only), FlatBuffers and Cap'n Proto. It is a module of its own, so that the
two runtimes do not reach the `bench` module or the root one. Its tests run in CI
(`go vet ./... && go test ./...`); nothing is generated there.

```sh
cd bench/formats
go test ./...                                                              # the round trips
go test -run '^$' -bench . -benchmem -count 10 -timeout 60m | tee out.txt  # the benchmarks
benchstat -col /format -row /type,/op,/how out.txt                         # the report
```

(`benchstat` is `~/go/bin/benchstat`.) `-col /format -row /type,/op,/how` gives a table per unit
(sec/op, B/op, allocs/op, bytes/rec) with a row per type, op and how, and a column per format.
Not `-row .name`: the name of every row is `BenchmarkFormats`, so it collapses them all into one.
To look at one `how`, add a filter: `benchstat -filter '/how:partial' -col /format -row /type,/op out.txt`
(`-filter '/op:read-scalar'` and `-filter '/type:doc'` work the same way). A quick run of the
partial rows only (the `-bench` pattern is split at each `/` of the name, so `how=partial` alone
matches nothing):

```sh
go test -run '^$' -bench 'Formats/type=/op=/how=partial' -benchmem -count 1 -benchtime 2000x
```

The benchmark names are `BenchmarkFormats/type=<order-small|order-large|book|doc>/op=<...>/how=<full|native|partial>/format=<...>`:
the fairness rules are at the top of `bench_test.go`.

## What `how` is

- `full`: the record of Go is built (every encode, and a read that decodes the record and then
  reads the field).
- `native`: the field is read from the bytes with the accessors of FlatBuffers or Cap'n Proto,
  and nothing is built.
- `partial`: kvutil's partial decoding, what a query does per record that it rejects on a field:
  `dec, ok := codec.PartialOf(c, []string{"Total"})` is prepared once (the setup fails the
  benchmark if `ok` is false, or if the decoded field differs from the source), and a record is
  `v = V{}`, `dec(bz, &v)` and the read of the field. The codecs are the ones a store is given:
  `codec.Formatted[T](codec.Msgpack)` (decodes into a narrower struct and skips the other fields
  of the map), `codec.Binary[OrderBin]` (the hand-written codec walks and skips the fields before
  the one it wants), `codec.Binary[OrderFB|BookFB|DocFB]` and `codec.Binary[OrderCapnp|...]`
  (`partial.go`: the accessor of the format straight from the bytes, and a copy of the field;
  a path that is not known decodes everything). `capnp` makes a message per record with
  `capnp.Unmarshal` (a store has no reader per iterator to reuse; that is the honest cost of
  this API), and `capnp-reuse` (`OrderCapnpReuse|...`) gets a `CapnpReader` from a `sync.Pool`
  to show what reusing the message would give. The `full` rows of flatbuffers and capnp are not
  repeated through `codec.Binary`: its `Decode` is the same decoder (and `capnp.Unmarshal`) and a
  copy of the struct.

## Generated code

`fbgen/` and `capgen/` are committed. The schemas are in `schema/`; the ids are not in them, as
kvutil keeps the id in the key. To generate them again, with flatc 25.12.19, Cap'n Proto 1.5.0 and
`capnpc-go` of `capnproto.org/go/capnp/v3 v3.1.0-alpha.2` (`go install capnproto.org/go/capnp/v3/capnpc-go@v3.1.0-alpha.2`,
which lands in `~/go/bin`), from this directory:

```sh
flatc --go --gen-object-api --go-namespace fbgen \
  --go-module-name github.com/ehsanranjbar/kvutil/bench/formats -o . schema/records.fbs

capnp compile -I "$(go list -m -f '{{.Dir}}' capnproto.org/go/capnp/v3)/std" \
  --src-prefix=schema -o"$HOME/go/bin/capnpc-go:capgen" schema/records.capnp
```

The first writes `fbgen/{Order,Book,Doc}.go` (the object API is generated, but the code uses the
accessors: the object API cannot tell an absent string from an empty one). The second writes
`capgen/records.capnp.go`; `--src-prefix` keeps `schema/` out of the output path, and `-I` is where
`/go.capnp` of the Go annotations is.

The generated FlatBuffers files are not `gofmt`-ed; the rest is.

## Mapping

- `time.Time` is Unix nanoseconds (`int64`); the dataset has second resolution in UTC, which a
  decode restores exactly.
- `*string` (`Order.Note`, `Book.Subtitle`): FlatBuffers has an absent field for nil and a present
  one, even empty, for non-nil. Cap'n Proto has no optional text, so a `Bool` (`noteSet`,
  `subtitleSet`) says whether the text is there. (Not `hasNote`: the Go generator already names the
  accessor of the text `HasNote`.)
- An empty `Tags`, `Payload` or `Embedding` is nil once decoded, as the dataset makes them.
- A decode copies out of its input, as the `ValueCodec` contract of kvutil says.
