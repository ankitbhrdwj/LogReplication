package cluster
//package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"io"
	"os"
	"strconv"
	"time"
	"sync"
)

//constants and variables

const (
	BROADCAST = -1
)

var hash map[int]string
type Envelope struct {
	Pid   int
	MsgId int64
	Msg   interface{}
}

type Server interface {
	Pids() int
	Peers() []int
	Outbox() chan *Envelope
	Inbox() chan *Envelope
}

type RaftServer struct {
	serverID      int
	serverAddress string
	peers         []int
	peerAddress   []string
	socket        *zmq.Socket
	wait          sync.WaitGroup
	inbox         chan *Envelope
	outbox        chan *Envelope
	msgid 	      int64
}

var server RaftServer

//functions Definitions

func (server RaftServer) Pids() int {
	return server.serverID
}

func (server RaftServer) Peers() []int {
	return server.peers
}

func (server RaftServer) Outbox() chan *Envelope {
	(&server).outbox = make(chan *Envelope,100)
	go sendMsg(&server)
	time.Sleep(80*time.Millisecond)
	return (&server).outbox
}

func (server RaftServer) Inbox() chan *Envelope {
	go getMsg(&server)
	time.Sleep(100*time.Millisecond)
	return (&server).inbox
}

func New(myPid int, ConFile string) *RaftServer {
	server := perServer_(myPid, ConFile)
	server.msgid=1
	server.socket, _ = zmq.NewSocket(zmq.PULL)
	server.socket.Bind("tcp://" + server.serverAddress)
	server.wait.Add(1)
	server.inbox = make(chan *Envelope,100)
	return server
}

func getMsg(server *RaftServer) {
	var msg Envelope
	b, _ := server.socket.RecvBytes(0)   
	err := json.Unmarshal(b, &msg)
	if err != nil {
		fmt.Println(err)
	} else {
		server.inbox <- &msg
	}
}

func sendMsg(server *RaftServer) {
	for msg := range server.outbox {
		PID := ((*msg).Pid)
		if PID != -1 {
			if PID == server.serverID {
				fmt.Println("Server is trying to send the msg to itself")
				return
			} else {
				
				((*msg).MsgId)=server.msgid
				server.msgid=server.msgid+1
				b, err := json.Marshal(msg)
				if err != nil {
				fmt.Println(err)
				}
				client, _ := zmq.NewSocket(zmq.PUSH)
				client.Connect("tcp://" + hash[PID])
				client.SendBytes(b, 0)
				defer client.Close()
			}
		} else {
			for i := range server.peers {
				PID := server.peers[i]
				if PID == server.serverID {
					continue
				} else {
					((*msg).Pid) = server.serverID
					((*msg).MsgId)=server.msgid
			  		server.msgid=server.msgid+1
					b, err := json.Marshal(msg)
					if err != nil {
					fmt.Println(err)
					}					
					client, _ := zmq.NewSocket(zmq.PUSH)
					client.Connect("tcp://" + hash[PID])
					client.SendBytes(b, 0)
					defer client.Close()
				}
			}

		}
	}
	server.wait.Done()
}

func perServer_(myPid int, ConFile string) *RaftServer {
	hash = make(map[int]string)
	server := new(RaftServer)
	file, err := os.Open(ConFile)
	if err != nil {
		fmt.Println("Error:", err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error:", err)
		}
		x, _ := strconv.Atoi(record[0])
		server.peers = append(server.peers, x)
		server.peerAddress = append(server.peerAddress, record[1])
		hash[x] = record[1]
	}
	server.serverID = myPid
	server.serverAddress = hash[myPid]
	return server
}

/*
func main(){
			var i int 
			var server [10]*RaftServer
			for i=0;i<10;i++ {
			j:=i+1
			server[i] = New(j, "config.txt")
			}

		go send(server)
		go recieve(server)
for{
	time.Sleep(10*time.Second)
}
}

func main(){
			totmsg:=0
			server1:=New(1,"config.txt")
			server2:=New(2,"config.txt")
			go func(){
			for i:=0;i<1000;i++{
			server1.Outbox() <- &Envelope{Pid:2, MsgId: 0, Msg: "Hello !!! Are you there"}
			time.Sleep(5*time.Millisecond)			
			}
			}()
			go func(){
			for{		
			select {
			case envelope := <- server2.Inbox():
				totmsg++
				fmt.Printf("Received msg-> \nfrom : %d\nMsg_id : %d \nMsg :'%s'\n==============================\n", 					envelope.Pid, envelope.MsgId, envelope.Msg)

			case <-time.After(2 * time.Second):
				println("Waited and waited. Ab thak gaya",totmsg)
}
}
}()
for{
time.Sleep(10*time.Second)
}
}
*/
