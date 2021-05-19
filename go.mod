module github.com/BrobridgeOrg/gravity-transmitter-postgres

go 1.13

require (
	github.com/BrobridgeOrg/gravity-api v0.2.12
	github.com/BrobridgeOrg/gravity-sdk v0.0.4
	github.com/go-sql-driver/mysql v1.5.0 // indirect
	github.com/jinzhu/copier v0.3.0
	github.com/jmoiron/sqlx v1.3.4
	github.com/lib/pq v1.10.1
	github.com/sirupsen/logrus v1.7.0
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/viper v1.7.1
	golang.org/x/net v0.0.0-20200625001655-4c5254603344
	google.golang.org/grpc v1.32.0
	google.golang.org/grpc/examples v0.0.0-20200807164945-d3e3e7a46f57 // indirect
)

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api

//replace github.com/BrobridgeOrg/gravity-sdk => ../gravity-sdk
