// models.go
package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func makeTimeStamp(cur time.Time) string {
	var t string
	var r []string
	t = fmt.Sprintf("%s", cur.Format(time.RFC3339))
	r = strings.Split(t, "+")
	return r[0]
}
func makeNowStamp() string {
	return makeTimeStamp(time.Now().UTC())
}

type dbConf struct {
	Kind       string
	Transport  string
	ServerIP   string
	ServerPort string
	DbName     string
	User       string
	Password   string
}

var dbCfg dbConf

func iniDatabase(cfg dbConf) bool {
	var rez = false
	strConnect := cfg.User + ":" + cfg.Password + "@" + cfg.Transport + "(" + cfg.ServerIP + ":" + cfg.ServerPort + ")/" + cfg.DbName
	log.Println("Database:", strConnect)
	db, err := sql.Open(cfg.Kind, strConnect)
	if err == nil {
		err = db.Ping()
		if err == nil {
			go managerDB(cfg)
			rez = true
			//-----------------------------
		} else {
			log.Println(err)
		}
		db.Close()
	} else {
		log.Println("Error:", err)
	}

	return rez
}

//--------------------addRecord-----------------------
// канал запроса
var addRecordAskChan chan [3]string

// канал ответа
var addRecordRezChan chan int

func addRecord(guid string, tm time.Time) {
	var u [3]string
	u[0] = guid
	u[1] = "created"
	u[2] = makeTimeStamp(tm)

	addRecordAskChan <- u
	<-addRecordRezChan
}

//---------------------editRecord------------------------
// канал запроса
var editRecordAskChan chan [3]string

// канал ответа
var editRecordRezChan chan int

func editRecord(guid string, status string, m time.Time) {
	var u [3]string
	u[0] = guid
	u[1] = status
	u[2] = makeTimeStamp(m)
	editRecordAskChan <- u
	<-editRecordRezChan
}

//-----------------------getRecord---------------------
// канал запроса
var getRecordAskChan chan string

// канал ответа
var getRecordRezChan chan grcrd

type grcrd struct {
	status    string
	timeStamp string
}

// получение информвции по uuid
func getRecord(guid string) grcrd {

	getRecordAskChan <- guid
	return <-getRecordRezChan
}

// инициализация всех каналов
func iniСhannels() {
	addRecordAskChan = make(chan [3]string)
	addRecordRezChan = make(chan int)
	editRecordAskChan = make(chan [3]string)
	editRecordRezChan = make(chan int)
	getRecordAskChan = make(chan string)
	getRecordRezChan = make(chan grcrd)
}

func abs(num int) int {
	if num < 0 {
		return num * (-1)
	}
	return num
}

// основная рутина обработчик запросов к базе
func managerDB(cfg dbConf) {

	ex := 0
	ex2 := 0
	var TimeStamp time.Time

	iniСhannels()

	for ex == 0 {
		strConnect := cfg.User + ":" + cfg.Password + "@" + cfg.Transport + "(" + cfg.ServerIP + ":" + cfg.ServerPort + ")/" + cfg.DbName

		db, err := sql.Open(cfg.Kind, strConnect)
		if err == nil {
			err = db.Ping()
			if err == nil {
				//*************************************************
				TimeStamp = time.Now().UTC()
				ex2 = 0
				for ex2 == 0 {
					select {

					case x := <-addRecordAskChan:
						go func() {
							r := 0
							stmt, err := db.Prepare("INSERT tasks SET guid=?,mstatus=?,mtime=?")
							if err == nil {
								res, err := stmt.Exec(x[0], x[1], x[2])
								if err == nil {
									id, errp := res.LastInsertId()
									if errp == nil {
										r = int(id)
									} else {
										r = -1
									}
								} else {
									fmt.Println(err)
									r = -2
								}
								stmt.Close()
							} else {
								fmt.Println(err)
								r = -3
							}
							addRecordRezChan <- r
						}()
					case x := <-getRecordAskChan:
						go func() {
							var unu grcrd
							var id int
							unu.status = "no"
							rows, err := db.Query("SELECT id,mstatus,mtime FROM tasks WHERE guid=? ;", x)
							if err == nil {
								for rows.Next() {
									err := rows.Scan(&id, &unu.status, &unu.timeStamp)
									if err != nil {
										fmt.Println(err)
									}
								}
								rows.Close()
							}
							getRecordRezChan <- unu
						}()
					case y := <-editRecordAskChan:
						go func() {
							var ru int
							if len(y) > 2 {
								str := "UPDATE tasks SET mstatus=?,mtime=? WHERE guid=?"
								stm, err := db.Prepare(str)
								if err == nil {
									_, err2 := stm.Exec(y[1], y[2], y[0])
									if err2 == nil {
										//fmt.Println(err2)
										ru = 1
									} else {
										ru = -1
									}
									stm.Close()
								} else {
									fmt.Println(err)
									ru = -2
								}
							}
							editRecordRezChan <- ru
						}()

					default:
						ddur := time.Since(TimeStamp)
						if int(ddur.Seconds()) >= 2 {
							// проверка состояния конекта к
							// базе
							err = db.Ping()
							if err == nil {
								TimeStamp = time.Now().UTC()
							} else {
								ex2 = 1
							}
						}
						time.Sleep(200 * time.Nanosecond)
					}
				}
				db.Close()
			} else {
				db.Close()
			}

		} else {
			log.Println("some bd error")
			log.Println(err.Error())
		}

	}
}

// область автоматически генерируемых обработчиков
//=*Models*

//-*Models*
