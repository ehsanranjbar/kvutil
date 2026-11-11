# bench/formats

A throwaway module that measures how the value formats compare for the stored records of kvutil,
to choose between FlatBuffers and Cap'n Proto: msgpack (the default of kvutil), the binary codec
written by hand (Order only), FlatBuffers and Cap'n Proto. It is a module of its own, so that the
two runtimes do not reach the `bench` module or the root one. Its tests run in CI
(`go vet ./... && go test ./...`); nothing is generated there.

```sh
cd bench/formats
go test ./...                                                      # the round trips
go test -run '^$' -bench . -benchmem -count 10 | tee formats.txt   # the benchmarks
~/go/bin/benchstat -col /format formats.txt                        # or -col /how, -col /type
```

The benchmark names are `BenchmarkFormats/type=<order-small|order-large|book|doc>/op=<...>/how=<native|full>/format=<...>`:
the fairness rules are at the top of `bench_test.go`.

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
