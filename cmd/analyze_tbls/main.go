package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"runtime"
	"smalltools/analyze_tbls"
	"smalltools/utils/logutil"
	"strconv"
	"strings"
	"time"
)

func main() {

	cfg, err := analyze_tbls.NewConfigWithFile("analyze_config.toml")
	if err != nil {
		fmt.Printf("config err = %v\n", err)
		return
	}

	err = logutil.InitLogger(cfg.LogLever, cfg.LogPath, false, nil)
	if err != nil {
		fmt.Printf("init log err = %v\n", err)
		return
	}

	runtime.GOMAXPROCS(cfg.WorkerCount)

	var hours []string
	var days []string

	// 每天的小时数
	for i := 0; i < 24; i++ {
		hours = append(hours, fmt.Sprintf("%02d", i))
	}

	log.Infof("hour = %v\n", hours)

	// 过去n天日期
	nArr := strings.Split(cfg.StartDay, "-")
	year, err := strconv.Atoi(nArr[0])
	month, err := strconv.Atoi(nArr[1])
	day, err := strconv.Atoi(nArr[2])
	nTime := time.Date(year, time.Month(month), day, 18, 0, 0, 0, &time.Location{})
	for i := 0; i < cfg.Period; i++ {
		nTime = nTime.AddDate(0, 0, -1)
		days = append(days, nTime.Format("2006-01-02"))
	}
	log.Infof("day = %v", days)

	job, _ := analyze_tbls.NewJob(*cfg)

	for _, day := range days {
		for _, hour := range hours {
			sql := fmt.Sprintf("select tbls from tidb_monitor.query_log where user not in ('root', 'monitor', "+
				"'zhujinlong', 'rongpei', 'sysnc_db', 'sys_syncspark') and start_day='%v' and start_hour='%v';", day, hour)
			job.JobCh <- sql
			//log.Info(sql)
		}
	}

	close(job.JobCh)

	job.RunRes(job.OpRes)

	for i := 0; i < cfg.WorkerCount; i++ {
		log.Info(i)
		job.RunDp(job.OpDataPiece)
	}

	job.Close()

	job.SaveRes()

	log.Info("Done !!!")
}
