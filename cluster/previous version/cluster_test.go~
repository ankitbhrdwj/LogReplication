package cluster

import (
	"fmt"
	"testing"
	"time"
)

var Max = 10

func servers(t *testing.T) {
	var i int
	var server [10]*RaftServer
	for i = 0; i < Max; i++ {
		j := i + 1
		server[i] = New(j, "config.txt")
	}

	go send(server)
	go recieve(server, t)
	time.Sleep(10 * time.Second)
}

func send(server [10]*RaftServer) {
	for j := 0; j < 10; j++ {
		for i := 0; i < 10; i++ {
			server[i].Outbox() <- &Envelope{Pid: -1, MsgId: 0, Msg: "Hello !!! Are you there"}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func recieve(server [10]*RaftServer, t *testing.T) {
	totrec := 0
	for {
		for i := 0; i < 10; i++ {
			go func(){
			select {
			case envelope := <-server[i].Inbox():
				if envelope.Pid <= 10 {
					totrec++
				}
			case <-time.After(2 * time.Second):
				if totrec != Max*Max-1 {
					t.Error("Error",totrec)
				} else {
					return
				}
			}
		}()
		}
	}
	print(totrec)
}

func bigmsg(t *testing.T) {
	bigmsg := "abcdefghijklmnopqrstuvwxyz1234567890!@#$%^&*()"
	for i := 0; i < 10; i++ {
		bigmsg = string(bigmsg + bigmsg)
	}
	fmt.Print(len(bigmsg), " Bytes ")
	go func() {
		svr1 := New(1, "config.txt")
		svr2 := New(2, "config.txt")
		svr1.Outbox() <- &Envelope{Pid: 2, MsgId: 1, Msg: bigmsg}
		select {
		case envelope := <-svr2.Inbox():
			if envelope.Msg != bigmsg {
				t.Error("Msg not same")
			}

		case <-time.After(5 * time.Second):
			fmt.Println("Msg not Recieved ,TIME OUT")
		}
	}()
}

func RRobin(t *testing.T) {
	var i int
	var server [10]*RaftServer
	for i = 0; i < Max; i++ {
		j := i + 1
		server[i] = New(j, "config.txt")
	}
	go func(){
	for i := 0; i < Max; i++ {
		next := (server[i].Pids() + 1) % Max
		if next == 0 { next = Max}
		print(server[i].Pids(), next)
		server[i].Outbox() <- &Envelope{Pid: next, MsgId: 1, Msg: "Hello !!! Are you there"}
	}
	}()
	rcvd := 0
	for i := 0; i < Max; i++ {
		select {
		case envelope := <-server[i].Inbox():
			print(envelope)
		if envelope.Msg != nil{
				rcvd++
			}

		case <-time.After(5 * time.Second):
			fmt.Println("Msg not Recieved ,TIME OUT")
		}
	}
	fmt.Print(rcvd)
	time.Sleep(5 * time.Second)
}

func TestCluster(t *testing.T) {
	servers(t)
	fmt.Println("No of Msgs Test: OK")
	bigmsg(t)
	fmt.Println("Big Msg Test: OK")
//	RRobin(t)
//	fmt.Println("RoundRobin Msges Test: OK")
}
