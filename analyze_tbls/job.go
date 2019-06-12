package analyze_tbls

import (
	"database/sql"
	"fmt"
	log "github.com/sirupsen/logrus"
	"smalltools/utils/mysql-cli"
	"strings"
	"sync"
	"time"
)

type Job struct {
	c Config

	dpWg  *sync.WaitGroup
	resWg *sync.WaitGroup

	JobCh  chan string
	resCh  chan string
	resMap map[string]int
}

func NewJob(c Config) (*Job, error) {

	j := &Job{}

	j.c = c

	j.dpWg = &sync.WaitGroup{}
	j.resWg = &sync.WaitGroup{}

	j.JobCh = make(chan string, 1000)
	j.resCh = make(chan string, 1000)
	j.resMap = make(map[string]int, 12000)

	return j, nil
}

func (j *Job) Close() {
	log.Info("prepare close")
	j.dpWg.Wait()

	log.Info("OpDataPiece goroutine close")
	close(j.resCh)
	j.resWg.Wait()

	log.Info("close")
}

func (j *Job) RunDp(f func()) {
	j.dpWg.Add(1)
	go func() {
		defer func() {
			log.Info("goroutine close !!!")
			j.dpWg.Done()
		}()
		f()
	}()
}

func (j *Job) RunRes(f func()) {
	j.resWg.Add(1)
	go func() {
		defer func() {
			log.Info("goroutine close !!!")
			j.resWg.Done()
		}()
		f()
	}()
}

func (j *Job) OpDataPiece() {

	for {
		select {
		case sql, ok := <-j.JobCh:
			if !ok {
				return
			}
			log.Info(sql)
			db, err := mysql_cli.GetConn(j.c.Dbcfg.User, j.c.Dbcfg.Password, j.c.Dbcfg.Host, j.c.Dbcfg.Port)
			if err != nil {
				log.Error(err)
				log.Errorf("err sql = %v", sql)
			} else {
				rows, err := db.Query(sql)
				if err != nil {
					log.Error(err)
					log.Errorf("err1 sql = %v", sql)
				}
				for rows.Next() {
					var tbl string
					err := rows.Scan(&tbl)
					if err == nil {
						tbl = strings.Replace(tbl, "`", "", -1)
						tblArr := strings.Split(tbl, ",")
						for _, str := range tblArr {
							j.resCh <- str
						}
					}

				}
				if rows != nil {
					//可以关闭掉未scan连接一直占用
					log.Info("row close !!!")
					if err := rows.Close(); err != nil {
						log.Error(err)
					}
				}
			}
		}
	}
}

func (j *Job) OpRes() {

	for {
		select {
		case res, ok := <-j.resCh:
			if !ok {
				return
			}
			if v, ok1 := j.resMap[res]; ok1 {
				j.resMap[res] = v + 1
			} else {
				j.resMap[res] = 1
			}
		}
	}

}

func (j *Job) SaveRes() {
	log.Info("SaveRes is running")
	sqlHead := fmt.Sprintf("insert into %v(db_name, table_name, table_frequency, period, statistical_day,"+
		"create_time, update_time) values(?,?,?,?,?,?,?)", j.c.TableName)
	period := 15
	statistical_day := "2019-06-10"
	create_time := time.Now().Format("2006-01-02 15:04:05")
	update_time := create_time

	sqlVals := [][]interface{}{}
	exec := func(db *sql.DB, sqlHead string, sqlVals *[][]interface{}) error {
		if len(*sqlVals) > 0 {
			err := mysql_cli.BatchInsert(db, sqlHead, *sqlVals)
			if err != nil {
				return err
			}
			*sqlVals = [][]interface{}{}
		}
		return nil
	}

	db, err := mysql_cli.GetConn(j.c.Dbcfg.User, j.c.Dbcfg.Password, j.c.Dbcfg.Host, j.c.Dbcfg.Port)
	if err != nil {
		log.Error(err)
	} else {
		for k, v := range j.resMap {
			var schema, table string
			st := strings.Split(k, ".")
			if len(st) == 2 {
				schema = st[0]
				table = st[1]
			} else if len(st) == 1 {
				schema = ""
				table = st[0]
			} else {
				log.Errorf("st error k = %v", st)
				continue
			}
			var sqlVal []interface{}
			sqlVal = append(sqlVal, schema)
			sqlVal = append(sqlVal, table)
			sqlVal = append(sqlVal, v)
			sqlVal = append(sqlVal, period)
			sqlVal = append(sqlVal, statistical_day)
			sqlVal = append(sqlVal, create_time)
			sqlVal = append(sqlVal, update_time)
			sqlVals = append(sqlVals, sqlVal)
			if len(sqlVals) > 500 {
				err := exec(db, sqlHead, &sqlVals)
				if err != nil {
					log.Errorf("SaveRes execute error !!! \n %v", err)
					log.Error(sqlVals)
					continue
				}
			}
		}
		err := exec(db, sqlHead, &sqlVals)
		if err != nil {
			log.Errorf("SaveRes execute error !!! \n %v", err)
			log.Error(sqlVals)
		}
	}

}
