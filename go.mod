module github.com/arcoloom/arco-worker

go 1.26.1

require (
	github.com/arcoloom/arco-proto v0.0.0-00010101000000-000000000000
	github.com/bodgit/sevenzip v1.6.1
	github.com/creack/pty v1.1.24
	github.com/klauspost/compress v1.18.5
	github.com/nwaples/rardecode v1.1.3
	github.com/ulikunitz/xz v0.5.15
	google.golang.org/grpc v1.79.2
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/bodgit/plumbing v1.3.0 // indirect
	github.com/bodgit/windows v1.0.1 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/spf13/afero v1.11.0 // indirect
	go4.org v0.0.0-20200411211856-f5505b9728dd // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
)

replace github.com/arcoloom/arco-proto => ../arco-proto
