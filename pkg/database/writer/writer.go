package writer

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	"github.com/BrobridgeOrg/gravity-transmitter-postgres/pkg/database"
	buffered_input "github.com/cfsghost/buffered-input"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var seq uint64

var (
	UpdateTemplate = `UPDATE "%s" SET %s WHERE "%s" = :primary_val`
	InsertTemplate = `INSERT INTO "%s" (%s) VALUES (%s)`
	DeleteTemplate = `DELETE FROM "%s" WHERE "%s" = :primary_val`
)

type DatabaseInfo struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Secure   bool   `json:"secure"`
	Username string `json:"username"`
	Password string `json:"password"`
	DbName   string `json:"dbname"`
}

type Writer struct {
	dbInfo            *DatabaseInfo
	db                *sqlx.DB
	commands          chan *DBCommand
	completionHandler database.CompletionHandler
	buffer            *buffered_input.BufferedInput
	tmpQueryStr       string
	handleQueryStr    string
}

func NewWriter() *Writer {
	writer := &Writer{
		dbInfo:            &DatabaseInfo{},
		commands:          make(chan *DBCommand, 2048),
		completionHandler: func(database.DBCommand) {},
		tmpQueryStr:       "",
		handleQueryStr:    "",
	}

	// Initializing buffered input
	opts := buffered_input.NewOptions()
	opts.ChunkSize = 100
	opts.ChunkCount = 10000
	opts.Timeout = 50 * time.Millisecond
	opts.Handler = writer.chunkHandler
	writer.buffer = buffered_input.NewBufferedInput(opts)

	return writer
}

func (writer *Writer) Init() error {

	// Read configuration file
	writer.dbInfo.Host = viper.GetString("database.host")
	writer.dbInfo.Port = viper.GetInt("database.port")
	writer.dbInfo.Secure = viper.GetBool("database.secure")
	writer.dbInfo.Username = viper.GetString("database.username")
	writer.dbInfo.Password = viper.GetString("database.password")
	writer.dbInfo.DbName = viper.GetString("database.dbname")

	log.WithFields(log.Fields{
		"host":     writer.dbInfo.Host,
		"port":     writer.dbInfo.Port,
		"secure":   writer.dbInfo.Secure,
		"username": writer.dbInfo.Username,
		"dbname":   writer.dbInfo.DbName,
	}).Info("Connecting to database")

	sslmode := "disable"
	if writer.dbInfo.Secure {
		sslmode = "enable"
	}

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		writer.dbInfo.Username,
		writer.dbInfo.Password,
		writer.dbInfo.Host,
		writer.dbInfo.Port,
		writer.dbInfo.DbName,
		sslmode,
	)

	// Open database
	db, err := sqlx.Open("postgres", connStr)
	if err != nil {
		log.Error(err)
		return err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	writer.db = db

	go writer.run()

	return nil
}

func (writer *Writer) chunkHandler(chunk []interface{}) {

	dbCommands := make([]*DBCommand, 0, len(chunk))

	for i, request := range chunk {

		req := request.(*DBCommand)

		if i == 0 {
			writer.handleQueryStr = req.QueryStr
		}

		if req.QueryStr == writer.handleQueryStr {

			dbCommands = append(dbCommands, req)

		} else {

			writer.handleQueryStr = req.QueryStr

			writer.processData(dbCommands)

			dbCommands = make([]*DBCommand, 0, len(chunk))
			dbCommands = append(dbCommands, req)
		}

	}

	if len(dbCommands) != 0 {
		writer.processData(dbCommands)
	}

}

func (writer *Writer) processInsertData(cmd *DBCommand, querys []string, args []interface{}) ([]string, []interface{}) {

	if writer.tmpQueryStr != cmd.QueryStr {
		writer.tmpQueryStr = cmd.QueryStr
		qStr, arg, _ := writer.db.BindNamed(cmd.QueryStr, cmd.Args)
		newQueryStr := ""
		lastIndex := 0
		for i := 1; i <= len(arg); i++ {
			newSeq := atomic.AddUint64((*uint64)(&seq), 1)
			key := fmt.Sprintf("%v%d", "$", i)
			newKey := fmt.Sprintf("%v%d", "$", newSeq)

			if i == 1 {
				index := strings.Index(qStr, key)
				newQueryStr = fmt.Sprintf("%v%v", qStr[:index], newKey)
				lastIndex = index + len(key)
			} else {
				qStr = qStr[lastIndex:]
				index := strings.Index(qStr, key)
				if index == -1 {
					continue
				}
				newQueryStr = fmt.Sprintf("%v%v%v", newQueryStr, qStr[:index], newKey)
				lastIndex = index + len(key)
				if i == len(arg) {
					newQueryStr = fmt.Sprintf("%v%v", newQueryStr, qStr[lastIndex:])
				}
			}
		}
		querys = append(querys, newQueryStr)
		args = append(args, arg...)
	} else {

		querys, args = writer.appendInsertData(cmd, querys, args)
	}

	return querys, args

}

func (writer *Writer) appendInsertData(cmd *DBCommand, querys []string, args []interface{}) ([]string, []interface{}) {

	_, arg, _ := writer.db.BindNamed(cmd.QueryStr, cmd.Args)
	var addVal []string
	for i := 1; i <= len(arg); i++ {
		newSeq := atomic.AddUint64((*uint64)(&seq), 1)
		newKey := fmt.Sprintf("%v%d", "$", newSeq)
		addVal = append(addVal, newKey)
	}
	addVals := strings.Join(addVal, ",")
	newQuery := fmt.Sprintf("%s,(%s)", querys[len(querys)-1], addVals)
	querys[len(querys)-1] = newQuery
	args = append(args, arg...)

	return querys, args
}

func (writer *Writer) processUpdateData(cmd *DBCommand, querys []string, args []interface{}) ([]string, []interface{}) {

	if writer.tmpQueryStr != cmd.QueryStr {

		writer.tmpQueryStr = cmd.QueryStr
		qStr, arg, _ := writer.db.BindNamed(cmd.QueryStr, cmd.Args)
		newQueryStr := ""
		lastIndex := 0

		for i := 1; i <= len(arg); i++ {
			newSeq := atomic.AddUint64((*uint64)(&seq), 1)
			key := fmt.Sprintf("%v%d", "$", i)
			newKey := fmt.Sprintf("%v%d", "$", newSeq)

			if i == 1 {
				index := strings.Index(qStr, key)
				newQueryStr = fmt.Sprintf("%v%v", qStr[:index], newKey)
				lastIndex = index + len(key)
			} else {
				qStr = qStr[lastIndex:]
				index := strings.Index(qStr, key)
				if index == -1 {
					continue
				}

				newQueryStr = fmt.Sprintf("%v%v%v", newQueryStr, qStr[:index], newKey)
				lastIndex = index + len(key)
			}
		}

		querys = append(querys, newQueryStr)
		args = append(args, arg...)

	} else {

		querys, args = writer.appendUpdateData(cmd, querys, args)
	}

	return querys, args
}

func (writer *Writer) appendUpdateData(cmd *DBCommand, querys []string, args []interface{}) ([]string, []interface{}) {

	_, arg, _ := writer.db.BindNamed(cmd.QueryStr, cmd.Args)
	qStr := querys[len(querys)-1]
	qStr = fmt.Sprintf("%v;", qStr)
	if strings.Index(qStr, ");") == -1 {
		lastKey := fmt.Sprintf("%v%d", "$", seq)
		lastStr := fmt.Sprintf(" = %v;", lastKey)

		newSeq := atomic.AddUint64((*uint64)(&seq), 1)
		newKey := fmt.Sprintf("%v%d", "$", newSeq)
		inStr := fmt.Sprintf(" IN (%v,%v)", lastKey, newKey)

		qStr = strings.Replace(qStr, lastStr, inStr, 1)

	} else {
		newSeq := atomic.AddUint64((*uint64)(&seq), 1)
		newKey := fmt.Sprintf(",%v%d)", "$", newSeq)
		qStr = strings.Replace(qStr, ");", newKey, 1)
	}

	querys[len(querys)-1] = qStr
	args = append(args, arg[len(arg)-1])

	return querys, args
}

func (writer *Writer) processDeleteData(cmd *DBCommand, querys []string, args []interface{}) ([]string, []interface{}) {

	if writer.tmpQueryStr != cmd.QueryStr {
		writer.tmpQueryStr = cmd.QueryStr
		qStr, arg, _ := writer.db.BindNamed(cmd.QueryStr, cmd.Args)
		qStr = fmt.Sprintf("%v;", qStr)
		for i := 1; i <= len(arg); i++ {
			newSeq := atomic.AddUint64((*uint64)(&seq), 1)
			if i == len(arg) {
				key := fmt.Sprintf(" %v%d;", "$", i)
				newKey := fmt.Sprintf(" %v%d;", "$", newSeq)
				if key != newKey {
					qStr = strings.Replace(qStr, key, newKey, 1)
				}
			}
		}

		qStr = strings.TrimRight(qStr, ";")
		querys = append(querys, qStr)
		args = append(args, arg...)

	} else {

		querys, args = writer.appendDeleteData(cmd, querys, args)
	}

	return querys, args

}

func (writer *Writer) appendDeleteData(cmd *DBCommand, querys []string, args []interface{}) ([]string, []interface{}) {

	return writer.appendUpdateData(cmd, querys, args)
}

func (writer *Writer) processData(dbCommands []*DBCommand) {
	// Write to Database
	for {
		var args []interface{}
		var querys []string
		writer.tmpQueryStr = ""
		seq = 0
		for _, cmd := range dbCommands {

			switch cmd.Record.Method {
			case gravity_sdk_types_record.Method_INSERT:
				querys, args = writer.processInsertData(cmd, querys, args)

			case gravity_sdk_types_record.Method_UPDATE:
				querys, args = writer.processUpdateData(cmd, querys, args)

			case gravity_sdk_types_record.Method_DELETE:
				querys, args = writer.processDeleteData(cmd, querys, args)

			}

		}

		// Write to batch
		queryStr := strings.Join(querys, ";")

		_, err := writer.db.Exec(queryStr, args...)
		if err != nil {
			log.Error(err)
			log.Error(queryStr)

			<-time.After(time.Second * 5)

			log.WithFields(log.Fields{}).Warn("Retry to write record to database by batch ...")
			continue
		}

		break
	}

	for _, cmd := range dbCommands {
		writer.completionHandler(database.DBCommand(cmd))
	}
}

func (writer *Writer) run() {
	for {
		select {
		case cmd := <-writer.commands:
			// publish to buffered-input
			writer.buffer.Push(cmd)
		}
	}
}

func (writer *Writer) SetCompletionHandler(fn database.CompletionHandler) {
	writer.completionHandler = fn
}

func (writer *Writer) ProcessData(reference interface{}, record *gravity_sdk_types_record.Record) error {
	/*
		log.WithFields(log.Fields{
			"method": record.Method,
			"event":  record.EventName,
			"table":  record.Table,
		}).Info("Write record")
	*/
	switch record.Method {
	case gravity_sdk_types_record.Method_DELETE:
		return writer.DeleteRecord(reference, record)
	case gravity_sdk_types_record.Method_UPDATE:
		return writer.UpdateRecord(reference, record)
	case gravity_sdk_types_record.Method_INSERT:
		return writer.InsertRecord(reference, record)
	}

	return nil
}

func (writer *Writer) GetDefinition(record *gravity_sdk_types_record.Record) (*gravity_sdk_types_record.RecordDef, error) {

	recordDef := &gravity_sdk_types_record.RecordDef{
		HasPrimary: false,
		Values:     make(map[string]interface{}),
		ColumnDefs: make([]*gravity_sdk_types_record.ColumnDef, 0, len(record.Fields)),
	}

	// Scanning fields
	for n, field := range record.Fields {

		value := gravity_sdk_types_record.GetValue(field.Value)

		// Primary key
		//		if field.IsPrimary == true {
		if record.PrimaryKey == field.Name {
			recordDef.Values["primary_val"] = value
			recordDef.HasPrimary = true
			recordDef.PrimaryColumn = field.Name
			continue
		}

		// Generate binding name
		bindingName := fmt.Sprintf("val_%s", strconv.Itoa(n))
		recordDef.Values[bindingName] = value

		// Store definition
		recordDef.ColumnDefs = append(recordDef.ColumnDefs, &gravity_sdk_types_record.ColumnDef{
			ColumnName:  field.Name,
			Value:       field.Name,
			BindingName: bindingName,
		})
	}

	if len(record.PrimaryKey) > 0 && !recordDef.HasPrimary {
		log.WithFields(log.Fields{
			"column": record.PrimaryKey,
		}).Error("Not found primary key")

		return nil, errors.New("Not found primary key")
	}

	return recordDef, nil
}

func (writer *Writer) InsertRecord(reference interface{}, record *gravity_sdk_types_record.Record) error {

	recordDef, err := writer.GetDefinition(record)
	if err != nil {
		return err
	}

	return writer.insert(reference, record, record.Table, recordDef)
}

func (writer *Writer) UpdateRecord(reference interface{}, record *gravity_sdk_types_record.Record) error {

	recordDef, err := writer.GetDefinition(record)
	if err != nil {
		return err
	}

	// Ignore if no primary key
	if recordDef.HasPrimary == false {
		return nil
	}

	_, err = writer.update(reference, record, record.Table, recordDef)
	if err != nil {
		return err
	}

	return nil
}

func (writer *Writer) DeleteRecord(reference interface{}, record *gravity_sdk_types_record.Record) error {

	if record.PrimaryKey == "" {
		// Do nothing
		return nil
	}

	for _, field := range record.Fields {

		// Primary key
		//		if field.IsPrimary == true {
		if record.PrimaryKey == field.Name {

			value := gravity_sdk_types_record.GetValue(field.Value)

			sqlStr := fmt.Sprintf(DeleteTemplate, record.Table, field.Name)

			writer.commands <- &DBCommand{
				Reference: reference,
				Record:    record,
				QueryStr:  sqlStr,
				Args: map[string]interface{}{
					"primary_val": value,
				},
			}

			break
		}
	}

	return nil
}

func (writer *Writer) update(reference interface{}, record *gravity_sdk_types_record.Record, table string, recordDef *gravity_sdk_types_record.RecordDef) (bool, error) {

	// Preparing SQL string
	updates := make([]string, 0, len(recordDef.ColumnDefs))
	for _, def := range recordDef.ColumnDefs {
		updates = append(updates, `"`+def.ColumnName+`" = :`+def.BindingName)
	}

	updateStr := strings.Join(updates, ",")
	sqlStr := fmt.Sprintf(UpdateTemplate, table, updateStr, recordDef.PrimaryColumn)

	writer.commands <- &DBCommand{
		Reference: reference,
		Record:    record,
		QueryStr:  sqlStr,
		Args:      recordDef.Values,
	}

	return false, nil
}

func (writer *Writer) insert(reference interface{}, record *gravity_sdk_types_record.Record, table string, recordDef *gravity_sdk_types_record.RecordDef) error {

	paramLength := len(recordDef.ColumnDefs)
	if recordDef.HasPrimary {
		paramLength++
	}

	// Allocation
	colNames := make([]string, 0, paramLength)
	valNames := make([]string, 0, paramLength)

	if recordDef.HasPrimary {
		colNames = append(colNames, `"`+recordDef.PrimaryColumn+`"`)
		valNames = append(valNames, ":primary_val")
	}

	// Preparing columns and bindings
	for _, def := range recordDef.ColumnDefs {
		colNames = append(colNames, `"`+def.ColumnName+`"`)
		valNames = append(valNames, `:`+def.BindingName)
	}

	// Preparing SQL string to insert
	colsStr := strings.Join(colNames, ",")
	valsStr := strings.Join(valNames, ",")
	insertStr := fmt.Sprintf(InsertTemplate, table, colsStr, valsStr)

	//	database.db.NamedExec(insertStr, recordDef.Values)
	writer.commands <- &DBCommand{
		Reference: reference,
		Record:    record,
		QueryStr:  insertStr,
		Args:      recordDef.Values,
	}

	return nil
}
