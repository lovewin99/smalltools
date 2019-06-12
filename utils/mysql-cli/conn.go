package mysql_cli

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

const (
	retryNum = 10
)

func GetConn(user, pw, host, port string) (*sql.DB, error) {
	var db *sql.DB
	var err error
	for i := 0; i < retryNum && db == nil; i++ {
		db, err = sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/", user, pw, host, port))
		if err != nil {
			log.Infof("get conn error : retry times *v \n %v", i, err)
			time.Sleep(3 * time.Second)
		}
	}

	return db, err
}

func BatchInsert(db *sql.DB, sqlHead string, sqlVals [][]interface{}) error {
	var (
		tx  *sql.Tx
		err error
	)

	errFunc := func(i int, err error, sql string, v []interface{}) {
		if strings.Contains(err.Error(), "connection refused") {
			time.Sleep(3 * time.Second)
			log.Infof("connect tidb retry 100 times: %v\n", i+1)
		} else {
			log.Infof("sql exec error sql = %v %v, error info = %v\n", sql, v, err)
		}

	}

	isFirst := true

	for i := 0; i < retryNum && (isFirst || err != nil && (strings.Contains(err.Error(), "connection refused") ||
		strings.Contains(err.Error(), "bad connection"))); i++ {
		isFirst = false
		tx, err = db.Begin()
		if err != nil {
			errFunc(i, err, "begin", []interface{}{})
			continue
		}
		for _, sqlVal := range sqlVals {
			//log.Infof("exec dml sql = %v %v", sqlHead, sqlVal)
			_, err = tx.Exec(sqlHead, sqlVal...)
			if err != nil {
				errFunc(i, err, sqlHead, sqlVal)
				break
			}
		}
		if err == nil {
			tx.Commit()
		}
	}
	return err
}
