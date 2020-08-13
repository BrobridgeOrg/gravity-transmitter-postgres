module github.com/BrobridgeOrg/gravity-transmitter-postgres

go 1.13

require (
	github.com/BrobridgeOrg/gravity-api v0.0.0-20200810192326-098209cf878f
	github.com/denisenkom/go-mssqldb v0.0.0-20200620013148-b91950f658ec // indirect
	github.com/godror/godror v0.19.1 // indirect
	github.com/jmoiron/sqlx v1.2.0
	github.com/lib/pq v1.8.0
	github.com/nats-io/nats-streaming-server v0.18.0 // indirect
	github.com/sirupsen/logrus v1.6.0
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/viper v1.7.1
	golang.org/x/net v0.0.0-20190620200207-3b0461eec859
	google.golang.org/grpc v1.31.0
	google.golang.org/grpc/examples v0.0.0-20200807164945-d3e3e7a46f57 // indirect
)

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api
