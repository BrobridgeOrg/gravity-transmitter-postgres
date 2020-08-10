package app

import (
	"github.com/BrobridgeOrg/gravity-transmitter-postgres/pkg/database"
	"github.com/BrobridgeOrg/gravity-transmitter-postgres/pkg/grpc_server"
	"github.com/BrobridgeOrg/gravity-transmitter-postgres/pkg/mux_manager"
)

type App interface {
	GetGRPCServer() grpc_server.Server
	GetMuxManager() mux_manager.Manager
	GetWriter() database.Writer
}
