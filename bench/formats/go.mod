module github.com/ehsanranjbar/kvutil/bench/formats

go 1.27

replace github.com/ehsanranjbar/kvutil => ../..

replace github.com/ehsanranjbar/kvutil/bench => ..

require (
	capnproto.org/go/capnp/v3 v3.1.0-alpha.2
	github.com/ehsanranjbar/kvutil v0.0.0
	github.com/ehsanranjbar/kvutil/bench v0.0.0-00010101000000-000000000000
	github.com/google/flatbuffers v25.12.19+incompatible
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/colega/zeropool v0.0.0-20230505084239-6fb4a4f75381 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
