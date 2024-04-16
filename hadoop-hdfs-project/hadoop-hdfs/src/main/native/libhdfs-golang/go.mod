module hopsworks.ai/libhdfs

go 1.20

require (
	github.com/colinmarc/hdfs/v2 v2.2.0
	go.uber.org/zap v1.26.0
)

require (
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.11.0 // indirect
	golang.org/x/net v0.12.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)

replace github.com/colinmarc/hdfs/v2 v2.2.0 => github.com/logicalclocks/hopsfs-go-client/v2 v2.5.3

//replace github.com/colinmarc/hdfs/v2 v2.2.0 => /home/salman/code/hops/hopsfs-go/hopsfs-go-client
