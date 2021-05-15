package app

import (
	"github.com/BrobridgeOrg/gravity-transmitter-postgres/pkg/database"
)

type App interface {
	GetWriter() database.Writer
}
