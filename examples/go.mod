// A module of its own so the examples' dependencies stay out of the library's
// graph. The benchmark needs a TOML parser and a terminal reader to share its
// profile config with the Rust benchmark, and neither belongs in go.mod for a
// package that only stores and retrieves data.
module go.sia.tech/siastorage/examples

go 1.27.0

replace go.sia.tech/siastorage => ../

require (
	github.com/BurntSushi/toml v1.6.0
	github.com/vbauerster/mpb/v8 v8.16.3
	go.sia.tech/core v0.21.8
	go.sia.tech/siastorage v0.0.0-00010101000000-000000000000
	go.uber.org/zap v1.28.0
	golang.org/x/term v0.47.0
)

require (
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d // indirect
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/dunglas/httpsfv v1.1.1 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/reedsolomon v1.14.2 // indirect
	github.com/mattn/go-runewidth v0.0.31 // indirect
	github.com/oschwald/geoip2-golang v1.13.0 // indirect
	github.com/oschwald/maxminddb-golang v1.13.1 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.63.0 // indirect
	github.com/quic-go/webtransport-go v0.13.0 // indirect
	github.com/vbauerster/cupwriter v0.0.5 // indirect
	go.sia.tech/coreutils v0.25.0 // indirect
	go.sia.tech/indexd v0.6.1 // indirect
	go.sia.tech/mux v1.5.4 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	lukechampine.com/frand v1.5.1 // indirect
)
