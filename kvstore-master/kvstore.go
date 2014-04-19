package main

import (
	Raft "Raft"
	"fmt"
	"github.com/jmhodges/levigo"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mutex sync.Mutex
var hash map[string]string
var id = 0

//for levelDB
var ro *levigo.ReadOptions
var wo *levigo.WriteOptions
var db *levigo.DB

//var itr *levigo.Iterator

func main() {
	Port := ":"
	Args := len(os.Args)
	if Args != 3 {
		fmt.Print("Usage:go run kvstore <1-10> <port>\n")
		os.Exit(0)
	}
	Id, _ := strconv.Atoi(os.Args[1])
	id = Id
	Port = Port + os.Args[2]
	mutex.Lock()
	hash = make(map[string]string)
	hash["first"] = "demo"
	mutex.Unlock()
	http.HandleFunc("/index", index)
	http.HandleFunc("/insert/", insert)
	http.HandleFunc("/deletekv/", deletekv)
	http.HandleFunc("/check/", check)
	http.HandleFunc("/store/", store)
	go Raftlayer(Id)
	initDB(os.Args[1])
	http.ListenAndServe(Port, nil)
}

func index(res http.ResponseWriter, req *http.Request) {
	res.Header().Set("Content-Type", "text/html")
	io.WriteString(res,
		`<doctype html>
<html>
<head>
<title>kvstore</title>
</head>
<body>
<marquee BEHAVIOR=ALTERNATE BGCOLOR="#66CCFF"><h2><center>KVSTORE-HTTP SERVER IS WORKING !!!<center></h2></marquee>
<ul>
<li><h3>http://localhost:8080/insert/KEY=VALUE</h3>
<li><h3>http://localhost:8080/deletekv/KEY</h3>
<li><h3>http://localhost:8080/check/KEY</h3>
<li><h3>http://localhost:8080/store/</h3>/store/ To get the full KVSTORE
</ul>
</body>
<footer><blink><sup>**</sup>Key and Value can be of string type only.And key-value store work properly for the above given commands.</blink>
</html>`,
	)
}

func insert(res http.ResponseWriter, req *http.Request) {
	if raftnode.Leader() != 0 {
		key := strings.Split(req.URL.Path[8:], "=")
		var Key, Value string
		if key[0] != "" {
			Key = key[0]
		}
		if key[1] != "" {
			Value = key[1]
		}
		if Key != "" && Value != "" {
			if raftnode.Leader() != raftnode.NodePid {
				fmt.Fprintf(res, "This node is not the Leader Node,go to Node No %d", raftnode.Leader())
				return
			}
			mutex.Lock()
			//before inserting it in the hash insert it into the log
			operation := insertlog(Key, Value)
			operation = operation + " "
			Data := strings.Split(operation, " ")
			if Data[0] == "Insert" {
				key := strings.Split(Data[1], "=")
				value := strings.Split(Data[2], "=")
				if Key == key[1] && Value == value[1] {
					hash[Key] = Value
					err := db.Put(wo, []byte(Key), []byte(Value))
					if err != nil {
						log.Println(err)
					}
					fmt.Fprintf(res, "KEY = %s \t VALUE = %s", Key, Value)
				}
			}
			mutex.Unlock()
		} else {
			fmt.Fprintf(res, "Give argumets properly in the form  KEY=VALUE")
		}
	} else {
		res.Header().Set("Content-Type", "text/html")
		io.WriteString(res,
			`<doctype html>
<html>
<head>
<title>kvstore</title>
</head>
<body>
<marquee BEHAVIOR=ALTERNATE BGCOLOR="#66CCFF"><h2><center>KVSTORE-HTTP SERVER IS WORKING !!!<center></h2></marquee>
<h5 align="center">Some Serious stuff is going on !!!<br><br>Wait till the election process is completed<h5>
</body>
</html>`,
		)
	}
}

func deletekv(res http.ResponseWriter, req *http.Request) {
	if raftnode.Leader() != 0 {
		Key := strings.Split(req.URL.Path[10:], "\n")
		mutex.Lock()
		if Key[0] != "" && hash[Key[0]] != "" {
			if raftnode.Leader() != raftnode.NodePid {
				fmt.Fprintf(res, "This node is not the Leader Node,go to Node No. %d", raftnode.Leader())
				return
			}
			//before deleting from the hash ,append the delete entry in the log
			operation := deletelog(Key[0])
			operation = operation + " "
			Data := strings.Split(operation, " ")
			if Data[0] == "Delete" {
				key := strings.Split(Data[1], "=")
				if Key[0] == key[1] {
					fmt.Fprintf(res, "Pair deleted is KEY=%s \t VALUE=%s", Key[0], hash[Key[0]])
					delete(hash, Key[0])
					err := db.Delete(wo, []byte(Key[0]))
					if err != nil {
						log.Println(err)
					}
				}
			}
		} else {
			fmt.Fprintf(res, "Given KEY=VALUE pair is not available in KVSTORE")
		}
		mutex.Unlock()
	} else {
		res.Header().Set("Content-Type", "text/html")
		io.WriteString(res,
			`<doctype html>
<html>
<head>
<title>kvstore</title>
</head>
<body>
<marquee BEHAVIOR=ALTERNATE BGCOLOR="#66CCFF"><h2><center>KVSTORE-HTTP SERVER IS WORKING !!!<center></h2></marquee>
<h5 align="center">Some Serious stuff is going on !!!<br><br>Wait till the election process is completed<h5>
</body>
</html>`,
		)
	}
}

func check(res http.ResponseWriter, req *http.Request) {
	key := strings.Split(req.URL.Path[7:], "\n")
	mutex.Lock()
	if hash[key[0]] != "" {
		fmt.Fprintf(res, "KEY=%s \t VALUE=%s", key[0], hash[key[0]])
	} else {
		data, err := db.Get(ro, []byte(key[0]))
		if err != nil {
			fmt.Fprintf(res, "Given KEY=VALUE pair is not available in KVSTORE")
			log.Println(err)
		}
		fmt.Fprintf(res, "KEY=%s \t VALUE=%s", key[0], string(data))
	}
	mutex.Unlock()
}

func store(res http.ResponseWriter, req *http.Request) {
	mutex.Lock()
	for i := range hash {
		if hash[i] != "" {
			fmt.Fprintf(res, "KEY=%s \t VALUE=%s\n", i, hash[i])
		} else {
			fmt.Fprintf(res, "No value present in KVSTORE")
		}
	}
	mutex.Unlock()

}

func initDB(id string) {
	var err error
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 10))
	opts.SetCreateIfMissing(true)
	db, err = levigo.Open("level-DB"+id, opts)
	if err != nil {
		fmt.Print(err)
	}
	ro = levigo.NewReadOptions()
	wo = levigo.NewWriteOptions()
	//	itr = db.NewIterator(ro)
	defer ro.Close()
	defer wo.Close()
	//	defer itr.Close()
}

///////////////////////////////////////*Log Replication*/////////////////////////////////////
var raftnode *Raft.RaftNode

func Raftlayer(Id int) {
	raftnode = Raft.New(Id, "config.txt")
	go Listen(Id)
}

func Listen(Id int) {
	for {
		if (raftnode.Leader() != 0) && (raftnode.Leader() != Id) {
			operation := GetMsg()
			operation = operation + " "
			Data := strings.Split(operation, " ")
			mutex.Lock()
			if Data[0] == "Delete" {
				key := strings.Split(Data[1], "=")
				delete(hash, key[1])
				err := db.Delete(wo, []byte(key[1]))
				if err != nil {
					log.Println(err)
				}
			}
			if Data[0] == "Insert" {
				key := strings.Split(Data[1], "=")
				value := strings.Split(Data[2], "=")
				hash[key[1]] = value[1]
				err := db.Put(wo, []byte(key[1]), []byte(value[1]))
				if err != nil {
					log.Println(err)
				}
			}
			mutex.Unlock()
		}
		if (raftnode.Leader() != 0) && (raftnode.Leader() == Id) {
			time.Sleep(2 * time.Second)
		}
		if raftnode.Leader() == 0 {
			time.Sleep(5 * time.Second)
		}
	}
}

func GetMsg() string {
	var Msg interface{}
	var y Raft.LogItem
	for {
		for msg := range raftnode.Inbox() {
			if msg != nil {
				Msg = *msg
				y = Msg.(Raft.LogItem)
				break
			}
		}
		break
	}
	return y.Data.(string)
}

func insertlog(Key string, Value string) string {
	Msg := "Insert " + "Key=" + Key + " Value=" + Value
	raftnode.Outbox() <- Msg
	return GetMsg()
}

func deletelog(Key string) string {
	Msg := "Delete Key=" + Key
	raftnode.Outbox() <- Msg
	return GetMsg()
}
