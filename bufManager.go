// bufManager.go
package main

import (
	"time"
)

// поиск позиции в сортированном slice
// r=0 позиция найдена ind - индекс
// r=100 найдено место для вставки между на позицию ind
// r=-1 вставлять 1-й ы слайс
// r=1 вставить в конец слайса
func findGuuidPos(mstr []uint8, pie *[5]uint32) (r int, ind uint32) {
	// pie - переменные для избежания аллокаций
	pie[0] = uint32(len(myStructs))
	pie[1] = 0
	if pie[0] > 1 {
		pie[4] = pie[0] - 1

		for pie[3] = 0; pie[1] == 0; {
			// поиск метедом деления на 2
			if string(mstr) > string(myStructs[pie[3]].guid) && string(mstr) < string(myStructs[pie[4]].guid) {
				pie[2] = (pie[4] - pie[3]) >> 1
				if pie[2] == 0 {
					r = 100 //место для вставки
					ind = pie[4]
					pie[1] = 1
				} else {
					if string(mstr) < string(myStructs[pie[3]+pie[2]].guid) {
						pie[4] = pie[4] - pie[2]
					} else {
						if string(mstr) == string(myStructs[pie[3]+pie[2]].guid) {
							//нашли
							r = 0
							ind = pie[3] + pie[2]
							pie[1] = 1
						} else {
							if string(mstr) > string(myStructs[pie[3]+pie[2]].guid) {
								pie[3] += pie[2]
							}
						}

					}
				}
			} else {
				if string(mstr) < string(myStructs[pie[3]].guid) {
					r = -1
					pie[1] = 1
					ind = pie[3]
				} else {
					if string(mstr) == string(myStructs[pie[3]].guid) {
						r = 0
						pie[1] = 1
						ind = pie[3]
					} else {
						if string(mstr) > string(myStructs[pie[4]].guid) {
							r = 1
							pie[1] = 1
							ind = pie[4]
						} else {
							r = 0
							pie[1] = 1
							ind = pie[4]
						}
					}
				}
			}
		}
	} else { // обработка начального формирования
		if pie[0] == 1 {
			switch {
			case string(mstr) == string(myStructs[0].guid):
				r = 0
				ind = 0
			case string(mstr) < string(myStructs[0].guid):
				r = -1
				ind = 0
			case string(mstr) > string(myStructs[0].guid):
				r = 1
				ind = 1
			}
		} else { // пустой слайс
			r = 1
			ind = 0
		}

	}
	return
}

// добавление позиции в сортированный список slices
// substr - guid
func paddGuuid(substr []uint8) {
	var pos uint32
	var us int
	var dString mstru
	var mbuf [5]uint32

	if len(substr) > 0 {
		//готовим структуру для записи в слайс
		dString.guid = substr
		dString.status = 0
		dString.timeStamp = time.Now().UTC()
		us, pos = findGuuidPos(substr, &mbuf)

		switch us {
		case 0: //найден
			myStructs = append(myStructs, dString)
			copy(myStructs[pos+1:], myStructs[pos:])
			myStructs[pos] = dString
		case -1: //слева
			myStructs = append(myStructs, dString)
			copy(myStructs[1:], myStructs[0:])
			myStructs[0] = dString
		case 1: //справа
			myStructs = append(myStructs, dString)
		case 100: // между
			myStructs = append(myStructs, dString)
			copy(myStructs[pos+1:], myStructs[pos:])
			myStructs[pos] = dString
		}
	}
}

// структура основного слайса
// сортировка по guid
type mstru struct {
	guid      []uint8
	timeStamp time.Time
	status    uint8
}

// вспомогательная , подготовка ответа
type mstwu struct {
	guid      []uint8
	timeStamp string
	status    string
}

// основной слайс буфера
var myStructs []mstru

// структура данных отправляемых по основному каналу
type llch struct {
	ind   int     // индекс хандл-обработчика
	kind  uint8   // тип команды
	value []uint8 // содержимое(данные)
}

// основной канал команд
var mchan chan llch

// каналы emergency (закрытие программы)
var emergency chan int
var emergencydone chan int

// менеджер обработки буферных данных
func managerBuf() {
	var t, i int
	var gf grcrd
	var u mstwu
	var pie [5]uint32
	var v llch
	var ddur time.Duration
	// каналы emergency (закрытие программы)
	emergency = make(chan int)
	emergencydone = make(chan int)

	myStructs = make([]mstru, 0)
	// возможна инициализация по данным в DB
	// загрузить данные из DB со статусом не "finished"
	// как первоначальные для слайса !
	// Раз этого нет в задаче, то просто посылаю сигнал emergency
	// и ожидаю завершения всех task

	ex := 0
	for ex == 0 {
		select {
		case <-emergency:
			t = len(myStructs)
			for i = 0; i < t; {
				ddur = time.Since(myStructs[i].timeStamp)
				if int(ddur.Minutes()) > 1 {
					myStructs[i].status = 2
					myStructs[i].timeStamp = time.Now().UTC()
					editRecord(string(myStructs[i].guid), "finished", myStructs[i].timeStamp)
					//удаляем позицию из слайса
					myStructs = append(myStructs[:i], myStructs[i+1:]...)
				}
				t = len(myStructs)
			}
			ex = 1
			emergencydone <- 1
		case v = <-mchan:
			switch v.kind {
			case 1: //добавить в сортированный
				// статус created=0
				paddGuuid(v.value)
				u.guid = v.value
				nCh[v.ind].rchanP <- u
				go addRecord(string(v.value), time.Now().UTC())

			case 2: //получить из сортированного
				r, pos := findGuuidPos(v.value, &pie)
				if r == 0 {
					u.guid = myStructs[pos].guid
					switch myStructs[pos].status {
					case 0:
						u.status = "created"
					case 1:
						u.status = "running"
					case 2:
						u.status = "finished"
					}
					u.timeStamp = makeTimeStamp(myStructs[pos].timeStamp)
				} else {
					// если не найдено проверим базу
					gf = getRecord(string(v.value))
					if len(gf.status) > 1 {
						u.status = gf.status
						u.timeStamp = gf.timeStamp
					}
				}
				nCh[v.ind].rchanP <- u
			}
		default: // обрабатываем слайс
			t = len(myStructs)
			for i = 0; i < t; i++ {
				switch myStructs[i].status {
				case 0: //created сменить на статус running
					// и заменить таймстамп
					myStructs[i].timeStamp = time.Now().UTC()
					myStructs[i].status = 1
				case 1: //running ждем 2 минуты и меняем статус
					ddur = time.Since(myStructs[i].timeStamp)
					if int(ddur.Minutes()) > 1 {
						myStructs[i].status = 2
						myStructs[i].timeStamp = time.Now().UTC()
					}
				case 2: //finished удаляем из буфера
					myStructs[i].timeStamp = time.Now().UTC()
					go editRecord(string(myStructs[i].guid), "finished", myStructs[i].timeStamp)
					//addRecord(string(myStructs[i].guid), myStructs[i].timeStamp)
					//удаляем позицию из слайса
					myStructs = append(myStructs[:i], myStructs[i+1:]...)
				}
				// корректировка длины слайса с учетом удаленных
				t = len(myStructs)
			}
			// задержка для scheduler, чтобы освободить ресурсы
			time.Sleep(5 * time.Millisecond)

		}

	}
}
