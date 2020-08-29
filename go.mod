module github.com/BrobridgeOrg/gravity-transmitter-postgres

go 1.13

require (
	github.com/BrobridgeOrg/gravity-api v0.0.0-20200824082319-fe8e34a23ab9
	github.com/go-sql-driver/mysql v1.5.0 // indirect
	github.com/jmoiron/sqlx v1.2.0
	github.com/lib/pq v1.8.0
	github.com/sirupsen/logrus v1.6.0
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/viper v1.7.1
	golang.org/x/net v0.0.0-20190620200207-3b0461eec859
	golang.org/x/sys v0.0.0-20200202164722-d101bd2416d5 // indirect
	google.golang.org/grpc v1.31.0
	google.golang.org/grpc/examples v0.0.0-20200807164945-d3e3e7a46f57 // indirect
)

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api
