package main

import (
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

var cmd []*exec.Cmd
var portmap map[int]string
var Max int
var port int

func TestCluster(t *testing.T) {
	Max = 10
	port = 8080
	portmap = make(map[int]string)
	cmnd := exec.Command("go", "build", "kvstore.go")
	cmnd.Run()
	for i := 0; i < Max; i++ {
		cmd = append(cmd, exec.Command("./kvstore", strconv.Itoa(i+1), strconv.Itoa(port+i)))
	}
	for i := 0; i < Max; i++ {
		go run(i)
	}

	for k := 1; k <= 10; k++ {
		port = port + 1
		Port := strconv.Itoa(port)
		portmap[k] = Port
	}
	time.Sleep(50 * time.Second)
}

func run(i int) {
	cmd[i].Stdout = os.Stdout
	cmd[i].Stderr = os.Stderr
	cmd[i].Start()
}

func kill() {
	for i := 0; i < Max; i++ {
		cmd[i].Process.Kill()
	}
}

func LogEntryGenerator() {

}

/*
func kil_ldr() {
	err := cmd[j].Process.Kill()
	if err != nil {
		log.Println(err.Error())
	}
}
*/
