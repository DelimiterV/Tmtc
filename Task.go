// Task.go
package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// генерация uuid
func generateUUID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return uuid
}

// проверка на правильность uuid
func isValidUUID(u string) bool {
	_, err := uuid.Parse(u)
	return err == nil
}

// область переменных статически резервирующих память
// используется в хандлерах обработчиках
type lnnn struct {
	mych     chan int
	fch      chan uint8
	conn     net.Conn
	flag     int
	mflag    sync.Mutex
	rchanG   chan mstru
	rchanP   chan mstwu
	myerror  int
	bufR     []byte
	bufS     []byte
	ucontent []string
	tparams  []string
	uparams  []string
	mstr     string
}

// область Handle переменных для избежания аллокаций переменных
var nCh [nLINE]lnnn

// количество рутин обработчиков
const nLINE = 20

//====================================================
//text/html; charset=utf-8
//application/json
//заготовка для крафтера(сборщика) заголовка
var ppu = `Access-Control-Allow-Headers: Version, Authorization, Content-Type
Access-Control-Allow-Origin: *
Connection: close
Content-Type: `
var ppu2 = `Content-Length:`
var ppu3 = "\n\n"

//создание http пакета
func createPacket(kod string, mtype string, mcontent string) (rez string) {
	rez = "HTTP/1.1 " + kod + "\n"
	rez += ppu
	rez += mtype
	g := len(mcontent) + 2
	if g > 0 {
		rez += "\n" + ppu2
		rez += strconv.Itoa(g)
		rez += ppu3
		rez += mcontent
	} else {
		rez += ppu3
	}
	rez += "    "
	return
}

// длины резервируемых буферов
const sendLIMIT = 4000
const recvLIMIT = 4000

// рутина-handler основной обработчик
// запускается до tcp-handshake
func handleMicroConnection(ind int) {
	var i, j, zu, rlen int
	var err error
	var rstr string
	var params string
	var v llch
	ex := 0

	for ex != 1 {
		select {
		case <-nCh[ind].mych:
			rlen, err = nCh[ind].conn.Read(nCh[ind].bufR)

			if err == nil && rlen > 0 {
				zu = 0
				params = ""
				if rlen > 0 && ex != 1 {
					// блок парсинга заголовка ----------------
					for j = 0; j < rlen && nCh[ind].bufR[j] != ' ' && nCh[ind].bufR[j] != '\n'; j++ {
						zu += int(nCh[ind].bufR[j]) << uint(j)
					}
					j++
					for i = j; i < rlen && nCh[ind].bufR[i] != ' ' && nCh[ind].bufR[i] != '\n'; i++ {
						if nCh[ind].bufR[i] == '{' || nCh[ind].bufR[i] == '}' {
						} else {
							if nCh[ind].bufR[i] == '%' {
								i += 2
							} else {
								params += string(nCh[ind].bufR[i : i+1])

							}
						}
					}
					// -------end parsing------------------------
					// в переменной zu метод , в params URI(url)
					nCh[ind].uparams = strings.Split(params, "/")
					switch zu {
					case 545: //GET

						switch {
						case nCh[ind].uparams[1] == "task" && len(nCh[ind].uparams) == 3:
							//======= обработка GET запросa ==============
							if isValidUUID(nCh[ind].uparams[2]) {
								v.kind = 2
								v.ind = ind
								v.value = []uint8(nCh[ind].uparams[2])
								mchan <- v
								zzz := <-nCh[ind].rchanP
								if zzz.status != "no" {
									nCh[ind].mstr = `{
								"status": "` + zzz.status + `",
								"timestamp": "` + zzz.timeStamp + `"
							}    `
									rstr = createPacket("200 Ok", "text/html; charset=utf-8", nCh[ind].mstr)
									nCh[ind].bufS = []byte(rstr)
									_, _ = nCh[ind].conn.Write(nCh[ind].bufS[:len(nCh[ind].bufS)-1])

								} else {
									rstr = createPacket("404 Not Found", "text/html; charset=utf-8", "not found")
									nCh[ind].bufS = []byte(rstr)
									_, _ = nCh[ind].conn.Write(nCh[ind].bufS[:len(nCh[ind].bufS)-1])

								}

							} else {
								log.Println("invalid guid", nCh[ind].uparams[2])
								nCh[ind].mstr = createPacket("400 Bad Request", "application/json", "invalid guid")
								nCh[ind].bufS = []byte(nCh[ind].mstr)
								_, _ = nCh[ind].conn.Write(nCh[ind].bufS[:len(nCh[ind].bufS)-1])
							}
							//================ конец обработки GET =========
						default:
							rstr = createPacket("404 Not Found", "text/html; charset=utf-8", "")
							nCh[ind].bufS = []byte(rstr)
							_, _ = nCh[ind].conn.Write(nCh[ind].bufS[:len(nCh[ind].bufS)-1])

						}

					case 1242: //POST

						switch {

						case nCh[ind].uparams[1] == "task":
							//=============== обработка POST запроса ================
							v.kind = 1
							v.ind = ind
							v.value = []uint8(generateUUID())
							rstr := createPacket("202 Accepted", "text; charset=utf-8", string(v.value)+"  ")
							nCh[ind].bufS = []byte(rstr)
							_, _ = nCh[ind].conn.Write(nCh[ind].bufS[:len(nCh[ind].bufS)-2])
							mchan <- v
							<-nCh[ind].rchanP
							//=============== конец обработки POST =================
						default:

							rstr = createPacket("400 Bad Request", "text/html; charset=utf-8", "")
							nCh[ind].bufS = []byte(rstr)
							_, _ = nCh[ind].conn.Write(nCh[ind].bufS[:len(nCh[ind].bufS)-1])

						}
					default:
						rstr = createPacket("405 Method Not Allowed ", "text/html; charset=utf-8", "")
						nCh[ind].bufS = []byte(rstr)
						_, _ = nCh[ind].conn.Write(nCh[ind].bufS[:])

					}

				}

			}
			// закрываем соединение Connection: close
			nCh[ind].conn.Close()

		}
		// задержка для scheduler, чтобы освободить ресурсы
		time.Sleep(1 * time.Millisecond)
		// освобождение обработчика для новых flag=0
		nCh[ind].mflag.Lock()
		nCh[ind].flag = 0
		nCh[ind].mflag.Unlock()

	}

}

// Инициализация ,запуск прослушивания порта и распределение
// задач между рутинами-обработчиками
func initMicroServices(PORT string) {
	var counter int
	// подготовка статических переменных блока nCh
	// и запуск обработчиков
	for i := 0; i < nLINE; i++ {
		nCh[i].mych = make(chan int)
		nCh[i].fch = make(chan uint8)
		nCh[i].flag = 0
		nCh[i].rchanG = make(chan mstru)
		nCh[i].rchanP = make(chan mstwu)
		nCh[i].bufR = make([]byte, recvLIMIT)
		nCh[i].bufS = make([]byte, sendLIMIT)
		go handleMicroConnection(i)
	}
	log.Println("Microservice Tasks. Start to listen on ", PORT)
	ln, err := net.Listen("tcp", ":"+PORT)
	if err != nil {
		log.Println("Bind error:", err.Error())
	} else {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Println("Accept error:", err.Error())
			} else {
				ex := 0
				// поиск свободного обработчика
				for ex == 0 {
					// локальный counter c возможностью
					// обхода циклически
					if counter < nLINE-2 {
						counter++
					} else {
						counter = 0
					}
					nCh[counter].mflag.Lock()
					// flag=0 свободен
					if nCh[counter].flag == 0 {
						// послать задачу рутине обработчику
						// и пометить как "занятая" (flag=1)
						nCh[counter].flag = 1
						nCh[counter].conn = conn
						nCh[counter].mych <- counter
						ex = 1
					}
					nCh[counter].mflag.Unlock()
				}
			}
		}
	}
	ln.Close()
}

func main() {
	var input string
	// инициализация параметров коннекта к DB
	dbCfg.Kind = "mysql"
	dbCfg.DbName = "mtstask"
	dbCfg.ServerIP = "127.0.0.1"
	dbCfg.Password = "ghjcnjgbgtw"
	dbCfg.ServerPort = "3306"
	dbCfg.Transport = "tcp"
	dbCfg.User = "root"

	if iniDatabase(dbCfg) {
		//без базы не работаем
		mchan = make(chan llch)
		go managerBuf()
		log.Println("Hiload web-service style!(Post delay -10ms, Get delay 15 -ms")
		log.Println("Average CPU loading =1%, time base -UTC")
		go initMicroServices("80")
		// выходим по сигналу
		ex := 0
		for ex == 0 {
			time.Sleep(2 * time.Second)
			fmt.Print("Завершить? [Yy/Nn]:", "\n")
			fmt.Scanf("%s\r\n", &input)
			if input == "Y" || input == "y" {
				emergency <- 1
				ex = 1
			}
			input = ""
		}
		fmt.Println("Exit, wait...")
		<-emergencydone
	} else {
		log.Fatal("error init database service!")
	}

}
