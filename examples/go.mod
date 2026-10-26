// A module of its own so the examples' dependencies stay out of the library's
// graph. The benchmark needs a TOML parser and a terminal reader to share its
// profile config with the Rust benchmark, and neither belongs in go.mod for a
// package that only stores and retrieves data.
module go.sia.tech/siastorage/examples

go 1.27.0

replace go.sia.tech/siastorage => ../

require (
	github.com/BurntSushi/toml v1.6.0
	go.sia.tech/core v0.21.7
	go.sia.tech/siastorage v0.0.0-00010101000000-000000000000
	go.uber.org/zap v1.28.0
	golang.org/x/term v0.46.0
)

require (
	github.com/dunglas/httpsfv v1.1.0 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/reedsolomon v1.14.2 // indirect
	github.com/oschwald/geoip2-golang v1.13.0 // indirect
	github.com/oschwald/maxminddb-golang v1.13.1 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.61.0 // indirect
	github.com/quic-go/webtransport-go v0.12.0 // indirect
	go.sia.tech/coreutils v0.24.0 // indirect
	go.sia.tech/indexd v0.4.5-0.20260831084636-eb58007edc01 // indirect
	go.sia.tech/mux v1.5.3 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	lukechampine.com/frand v1.5.1 // indirect
)
