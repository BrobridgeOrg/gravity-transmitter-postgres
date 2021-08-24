module github.com/BrobridgeOrg/gravity-transmitter-postgres

go 1.13

require (
	github.com/BrobridgeOrg/gravity-sdk v0.0.40
	github.com/cfsghost/buffered-input v0.0.1
	github.com/jinzhu/copier v0.3.0
	github.com/jmoiron/sqlx v1.3.4
	github.com/lib/pq v1.10.1
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.7.1
	golang.org/x/crypto v0.0.0-20210711020723-a769d52b0f97 // indirect
	google.golang.org/genproto v0.0.0-20200624020401-64a14ca9d1ad // indirect
	google.golang.org/grpc v1.32.0 // indirect
)

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api

//replace github.com/BrobridgeOrg/gravity-sdk => ../gravity-sdk
