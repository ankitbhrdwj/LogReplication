package cluster

import (
	"fmt"
	"testing"
	"time"
)

var Max = 10
var totrec = 0

func servers(t *testing.T) int {
	var i int
	var server [10]*RaftServer
	for i = 0; i < Max; i++ {
		j := i + 1
		server[i] = New(j, "config.txt")
	}

	go send(server)
	go recieve(server, t)
	time.Sleep(10 * time.Second)
	RRobin(t, server)
	x := lateup(t, server)
	return x
}

func send(server [10]*RaftServer) {
	for j := 0; j < 250; j++ {
		for i := 0; i < 10; i++ {
			server[i].Outbox() <- &Envelope{Pid: -1, MsgId: 0, Msg: "Hello !!! Are you there"}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func recieve(server [10]*RaftServer, t *testing.T) {
	totrec = 0
	for {
		for i := 0; i < 10; i++ {
			select {
			case envelope := <-server[i].Inbox():
				if envelope.Pid <= 10 {
					totrec++
					//	println(totrec)
				}
			case <-time.After(2 * time.Second):
				if totrec != 250*Max*(Max-1) {
					t.Error("Error", totrec)
				} else {
					return
				}
			}
		}
	}
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

func RRobin(t *testing.T, server [10]*RaftServer) {
	go rrSend(server)
	go rrRecv(server, t)
	time.Sleep(3 * time.Second)
}

func rrSend(server [10]*RaftServer) {
	for i := 0; i < Max; i++ {
		next := (server[i].Pids() + 1) % Max
		if next == 0 {
			next = Max
		}
		server[i].Outbox() <- &Envelope{Pid: next, MsgId: 1, Msg: "Hello !!! Are you there"}
	}
}

func rrRecv(server [10]*RaftServer, t *testing.T) {
	rcvd := 0
	for i := 0; i < Max; i++ {
		select {
		case envelope := <-server[i].Inbox():
			if envelope.Pid <= 10 {
				rcvd++
			}

		case <-time.After(2 * time.Second):
			t.Error("Msg not Recieved ,TIME OUT")
		}
	}

	if rcvd != Max {
		t.Error()
	}
}

func lateup(t *testing.T, server [10]*RaftServer) int {
	tot := 100
	rcvd := 0

	go func() {
		for i := 0; i < tot; i++ {
			server[0].Outbox() <- &Envelope{Pid: 2, MsgId: 1, Msg: "Hello"}
		}
	}()

	time.Sleep(1 * time.Second)

	go func() {
		time.Sleep(1 * time.Second)
		for {
			select {
			case envelope := <-server[1].Inbox():
				if envelope != nil {
					rcvd++
				}

			case <-time.After(5 * time.Second):
				break
			}
		}
		if rcvd != tot {
			t.Error("Messages Lost due to late up")
		}
	}()
	time.Sleep(2 * time.Second)
	return rcvd
}

func TestCluster(t *testing.T) {
	x := servers(t)
	fmt.Println("No of Msgs :", totrec, " Test: OK")
	bigmsg(t)
	fmt.Println("Big Msg Test: OK")
	time.Sleep(1 * time.Second)
	fmt.Println("Round-Robin Msges Test: OK")
	fmt.Println(x, "Msgs delivered after Server Late up Test: OK")
}
