package cluster
import (
	"encoding/csv"
	"encoding/gob"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"io"
	"os"
	"strconv"
	"sync"
	"bytes"
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
	client        [10]*zmq.Socket
	wait          sync.WaitGroup
	mu            sync.Mutex
	inbox         chan *Envelope
	outbox        chan *Envelope
	msgid         int64
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
	return (&server).outbox
}

func (server RaftServer) Inbox() chan *Envelope {
	return (&server).inbox
}

func New(myPid int, ConFile string) *RaftServer {
	server := perServer_(myPid, ConFile)
	server.msgid = 1
	server.socket, _ = zmq.NewSocket(zmq.PULL)
	server.socket.Bind("tcp://" + server.serverAddress)
	server.wait.Add(10)
	server.outbox = make(chan *Envelope, 1000)
	server.inbox = make(chan *Envelope, 1000)
	connect(server)
	go getMsg(server)
	go sendMsg(server)
	return server
}

func getMsg(server *RaftServer) {
	for {
		var msg Envelope			// This statement,outside the loop was creating lots of trouble to me. 
		b, _ := server.socket.RecvBytes(0)
		newBuf := new(bytes.Buffer)
		dec := gob.NewDecoder(newBuf)
		newBuf.Write(b)
		err := dec.Decode(&msg)
		//fmt.Println("@cluster :",msg)
		//err := json.Unmarshal(b, &msg)
		if err != nil {
			fmt.Println(err)
		} else {
			server.inbox <- &msg
		}

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
				((*msg).Pid) = server.serverID
				if ((*msg).MsgId) == 0 {
					((*msg).MsgId) = server.msgid
					server.msgid++
				}
				buf := new(bytes.Buffer)
				enc := gob.NewEncoder(buf)
				err:=enc.Encode(msg)
				if err != nil {
					fmt.Println(err)
				}
				server.mu.Lock()
				if PID <10 {
				b := buf.Bytes()
				server.client[PID-1].SendBytes(b, 0)	//??????? Some problem is there.May be I need to modifiy this
				}				
				server.mu.Unlock()
			}
		} else {
			for i := range server.peers {
				PID := server.peers[i]
				if PID == server.serverID {
					continue
				} else {
					((*msg).Pid) = server.serverID
					if ((*msg).MsgId) == 0 {
						((*msg).MsgId) = server.msgid
						server.msgid++
					}
					buf := new(bytes.Buffer)
					enc := gob.NewEncoder(buf)
					err:=enc.Encode(msg)
					if err != nil {
						fmt.Println(err)
					}
					server.mu.Lock()
					b := buf.Bytes()
					server.client[PID-1].SendBytes(b, 0)
					server.mu.Unlock()

				}
			}

		}
	}
	server.wait.Done()
}

func connect(server *RaftServer) {
	var err error
	for i := range server.peers {
		server.mu.Lock()
		server.client[i], err = zmq.NewSocket(zmq.PUSH)
		if err != nil {
			panic(err)
		}
	//	print("value of i :",i,"Value of address :",hash[server.peers[i]])
		server.client[i].Connect("tcp://" + hash[server.peers[i]])
		server.mu.Unlock()
	}
}

func (server RaftServer) conn_close() {
	for i := range (&server).client {
		defer (&server).client[i].Close()
	}
}

func perServer_(myPid int, ConFile string) *RaftServer {
	hash = make(map[int]string)
	server := new(RaftServer)
	file, err := os.Open(ConFile)
	if err != nil {
		fmt.Println("Error:", err)
	}
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
	file.Close()
	server.serverID = myPid
	server.serverAddress = hash[myPid]
	return server
}
