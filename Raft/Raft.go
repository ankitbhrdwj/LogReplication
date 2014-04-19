package Raft

//package main

import (
	"bufio"
	"cluster"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type RaftNode struct {
	Isflr      bool //follower
	Isldr      bool //leader
	Iscnd      bool //candidate
	NodePid    int  //PID of that node
	CurTerm    int  //Term of that particular node
	VotedTerm  int  //voted at what term
	MaxTime    int  //election timeout to avoid split voting
	ElecTO     int  //convert from follower to candidate if no msg arrived from leader
	LdrId      int  //PID of leader
	TotNodes   int  //total number of nodes for majority in election
	Votes      int  //total votes recieved
	TO_FLAG    int  //whether election should happen
	inbox      chan *LogItem
	outbox     chan interface{}
	raftserver *cluster.RaftServer
}

type Raft interface {
	Term() int64
	Leader() int
	Outbox() chan<- interface{} //here the msg will be forwarded to the followers if its a Leader
	Inbox() <-chan *LogItem     // here will get msg to be replicated
}

func (node RaftNode) Leader() int {
	return node.LdrId
}

func (node RaftNode) Term() int {
	return node.CurTerm
}

func (node RaftNode) isLeader() bool {
	return node.Isldr
}

func (node RaftNode) Outbox() chan interface{} {
	return (&node).outbox
}

func (node RaftNode) Inbox() chan *LogItem {
	return (&node).inbox
}

func getTerm(myPid int) int {
	bytes := make([]byte, 20)
	filename := "file" + strconv.Itoa(myPid) + ".txt"
	file, err := os.Open(filename)
	if err != nil {
		log.Println("error getting term")
	}
	defer file.Close()
	file.Read(bytes)
	curterm := strings.Split(string(bytes), "\n")
	value := curterm[0]
	Term, _ := strconv.Atoi(value)
	return Term
}

func setTerm(raftnode *RaftNode, term int) {
	raftnode.CurTerm = term
	/*
		Term := []byte(string(strconv.Itoa(term) + "\n"))
		filename := "file" + strconv.Itoa(raftnode.NodePid) + ".txt"
		err := ioutil.WriteFile(filename, Term, 0666)
		fmt.Println("open file ",filename)
		if err != nil {
			fmt.Println("error in file write ",err)
		}
	*/
}

func New(myPid int, ConFile string) *RaftNode {
	raftnode := perNode(myPid, ConFile)
	// register for gob types
	gob.Register(LogEntry{})
	gob.Register(LogReply{})
	gob.Register(LogItem{})
	gob.Register(cluster.Envelope{})
	go HBeatTO(raftnode)
	go GetMsg(raftnode)
	go LogReplicator(raftnode)
	return raftnode
}

func perNode(myPid int, ConFile string) *RaftNode {
	raftnode := new(RaftNode)
	raftnode.Isflr = true
	raftnode.Isldr = false
	raftnode.Iscnd = false
	raftnode.NodePid = myPid
	setTerm(raftnode, 1) //raftnode.CurTerm
	raftnode.VotedTerm = -1
	raftnode.MaxTime = 2000 //milli seconds
	raftnode.ElecTO = 4     //seconds
	raftnode.LdrId = 0
	raftnode.raftserver = clusterStart(myPid, ConFile, raftnode)
	raftnode.TotNodes = len(raftnode.raftserver.Peers())
	raftnode.Votes = 0
	raftnode.outbox = make(chan interface{}, 1000)
	raftnode.inbox = make(chan *LogItem, 1000)
	return raftnode
}

func clusterStart(myPid int, ConFile string, raftnode *RaftNode) *cluster.RaftServer {
	var srvr *cluster.RaftServer
	srvr = cluster.New(myPid, "config.txt")
	return srvr
}

func HBeatTO(raftnode *RaftNode) {
	for {
		raftnode.TO_FLAG = 1
		time.Sleep(time.Duration(raftnode.ElecTO) * time.Second)
		if raftnode.Isldr == true {
			raftnode.TO_FLAG = 0
		}
		if raftnode.Iscnd == true {
			raftnode.TO_FLAG = 0
		}
		if raftnode.TO_FLAG == 1 {
			if raftnode.Isldr != true {
				raftnode.Iscnd = true
				raftnode.Isflr = false
				//fmt.Println("Elections initiated by NodeID:", raftnode.NodePid)
				StartElection(raftnode)

			}
		}
	}
}

func GetMsg(raftnode *RaftNode) {
	for {
		scase := ""
		msg := ""
		select {
		case envelope := <-raftnode.raftserver.Inbox():
			Type := reflect.TypeOf(envelope.Msg)
			if "string" == Type.String() {
				msg = envelope.Msg.(string)
				msg1 := strings.Split(msg, " ")
				scase = msg1[0]
			} else {
				go logEntryHandler(envelope, raftnode)
			}

		case <-time.After(2 * time.Second):
			//fmt.Print("timeout @ GetMsg\n",raftnode.NodePid)   //For leader this may be the case that no one sends any msg to him
		}

		if len(scase) != 0 {
			switch scase {
			case "Candidate_ID:":
				{
					msg1 := strings.Split(msg, " ")
					Term, _ := strconv.Atoi(msg1[3])
					if raftnode.Isldr == false {
						if raftnode.Iscnd == true {
							if Term > raftnode.CurTerm {
								raftnode.CurTerm = Term
								raftnode.Iscnd = false
								raftnode.Isflr = true
							}
						}
						if raftnode.Isflr == true {
							//println(msg)
							go VoteReply(msg, raftnode)
						}
					}

				}
			case "Leader_ID:":
				{
					raftnode.TO_FLAG = 0
					msg1 := strings.Split(msg, " ")
					Term, _ := strconv.Atoi(msg1[3])
					LdrID, _ := strconv.Atoi(msg1[1])
					//println("hb msg ", msg1[1])
					if raftnode.CurTerm <= Term {
						raftnode.Isflr = true
						raftnode.Iscnd = false
						raftnode.Isldr = false
						raftnode.LdrId = LdrID
						raftnode.CurTerm = Term
					}
				}

			case "Follower_ID:":
				{
					if raftnode.Iscnd == true {
						//println(msg)
						msg1 := strings.Split(msg, " ")
						//Term,_ := strconv.Atoi(msg1[3])
						if msg1[4] == "Voted:" {
							raftnode.Votes++
							//fmt.Println("Votes :", raftnode.Votes)
							if raftnode.Votes >= (int(raftnode.TotNodes/2) + 1) { //Majority votes for him
								raftnode.Iscnd = false
								raftnode.Isldr = true
								raftnode.Isflr = false
								raftnode.LdrId = raftnode.NodePid
								//print("Leader Elected ", raftnode.LdrId, "\n")
								go SendHBeat(raftnode.CurTerm, raftnode.LdrId, raftnode)
							}
						}

						if msg1[4] == "NotVoted:" {
							raftnode.Isflr = true
							raftnode.Iscnd = false
							raftnode.Isldr = false
							raftnode.TO_FLAG = 0
						}
					}
				}
			default:
				{
					println("Invalid Msg\n", msg)
				}
			}
		}
	}
}

func StartElection(raftnode *RaftNode) {
	rand.Seed(time.Now().UnixNano())
	period := time.Duration(rand.Int63n(int64(raftnode.MaxTime))) //wait random amount of time before election
	//fmt.Println("random time ", period)
	time.Sleep(period * time.Millisecond)
	raftnode.LdrId = 0

	if raftnode.Isldr == false {
		if raftnode.Iscnd == true {
			Term := raftnode.CurTerm + 1
			raftnode.VotedTerm = Term
			raftnode.Votes = 1
			setTerm(raftnode, Term)
			//fmt.Println("Current term ",raftnode.CurTerm)
			go RequestVote(raftnode.CurTerm, raftnode.raftserver.Pids(), raftnode)
		}
	}
}

func RequestVote(Candterm int, candid int, raftnode *RaftNode) {
	//fmt.Println("give me votes\n")
	raftnode.raftserver.Outbox() <- &cluster.Envelope{Pid: -1, MsgId: 0, Msg: "Candidate_ID: " + strconv.Itoa(candid) + " Term: " + strconv.Itoa(Candterm) + " "}
}

func VoteReply(msg string, raftnode *RaftNode) {
	msg1 := strings.Split(msg, " ")
	CandID, _ := strconv.Atoi(msg1[1])
	Term, _ := strconv.Atoi(msg1[3])
	if raftnode.Isflr == true {
		if raftnode.VotedTerm >= Term {
			raftnode.raftserver.Outbox() <- &cluster.Envelope{Pid: CandID, MsgId: 0, Msg: "Follower_ID: " + strconv.Itoa(raftnode.NodePid) + " Term: " + strconv.Itoa(raftnode.CurTerm) + " NotVoted: " + " 1 "}
		} else {
			raftnode.VotedTerm = Term
			raftnode.raftserver.Outbox() <- &cluster.Envelope{Pid: CandID, MsgId: 0, Msg: "Follower_ID: " + strconv.Itoa(raftnode.NodePid) + " Term: " + strconv.Itoa(raftnode.CurTerm) + " Voted: " + " 1 "}
		}
	}

	if Term > raftnode.CurTerm {
		setTerm(raftnode, Term)
	}
}

func SendHBeat(term int, leaderid int, raftnode *RaftNode) {
	if raftnode.Isldr == true {
		go SendLog(raftnode)
	}
	for {
		if raftnode.Isldr == true {
			if raftnode.NodePid == leaderid {
				//fmt.Printf("Leader %d Sending HBM.....\n", leaderid)
				time.Sleep(1 * time.Second)
				raftnode.raftserver.Outbox() <- &cluster.Envelope{Pid: -1, MsgId: 0, Msg: "Leader_ID: " + strconv.Itoa(leaderid) + " Term: " + strconv.Itoa(term) + " "}
			}
		} else {
			return
		}
	}
}

func (node RaftNode) Exit() {
	os.Exit(0)
}

//////////////////////////////////////////////*Log Replication*///////////////////////////////////////////

type LogEntry struct {
	CommitIndex  int64
	Term         int
	LeaderId     int
	PrevLogindex int64
	PrevLogTerm  int
	LogCommand   LogItem
}

/*
type LogItem struct {
	Index int64
	Term  int
	Data  interface{} // The data that was supplied to raft's inbox
}
*/

type LogItem struct {
	Index int64       `json:"index"`
	Term  int         `json:"term"`
	Data  interface{} `json:"data"` // The data that was supplied to raft's inbox
}

type LogReply struct {
	Isat  int64
	Index int64
	Reply bool
	Data  interface{}
}

type Ack struct {
	positive int
	negative int
}

var logentry LogEntry
var item LogItem
var Log []LogItem
var BlockNext [10]bool //by default initialized to false and false means don't block
var CurrentIndex int64
var logIndex int64
var ACK map[int64]Ack //for which index Ack has come ???
var Majority = 6      // Depends on # of nodes in the cluster Majority=(Max/2+1)
var logFile string    //logfilename
var file *os.File     //log file descriptor

func LogReplicator(raftnode *RaftNode) {
	var err error
	ACK = make(map[int64]Ack)
	id := strconv.Itoa(raftnode.NodePid)
	logFile = "LogFile" + id + ".json"

	if _, err := os.Stat(logFile); err != nil {
		if os.IsNotExist(err) {
			os.Create(logFile) //????
		} else {
			fmt.Print(err)
		}
	}

	File, err := os.Open(logFile)
	if err != nil {
		log.Println("Error:", err)
	}
	scanner := bufio.NewScanner(File)
	var logItemBytes []byte
	var logExist = false
	var log LogItem
	logCount := 0
	for scanner.Scan() {
		logExist = true
		logItemBytes = []byte(scanner.Text())
		logCount++
	}
	if logExist {
		json.Unmarshal([]byte(logItemBytes), &log)
		//fmt.Println(logCount,log)
		logentry.CommitIndex = log.Index
		CurrentIndex = log.Index
		if len(Log) == 0 {
			Log = append(Log, log) //only last log entry will be sufficient to check previousindex and previousterm
			logIndex += 1
			//fmt.Println(Log)
		}
	}
	File.Close()

	file, err = os.OpenFile(logFile, os.O_RDWR|os.O_APPEND, 0660)
	if err != nil {
		fmt.Println(err.Error())
	}

}

func GetAck(item LogItem, raftnode *RaftNode) { //Msg to KVSTORE & somthing will come on cluster layer socket pass it to KVSTORE
	logentry.CommitIndex = item.Index
	//log.Println("Commited :",item)
	jsonBytes, err := json.Marshal(item)
	if err != nil {
		log.Println(err)
	}
	_, err = file.WriteString(string(jsonBytes) + "\n")
	if err != nil {
		log.Println(err)
	}
	raftnode.Inbox() <- &item
}

func SendLog(raftnode *RaftNode) { //Msg from KVSTORE & this function will replicate each value on the followers using last commited index
	for {
		for msg := range raftnode.outbox {
			if msg != nil && raftnode.Isldr == true {
				logObject := PrepareLogEntry(msg, raftnode)
				Log = append(Log, logObject.LogCommand)
				CurrentIndex += 1
				logIndex += 1
				//log.Println("Sent :",logObject)
				log.Println(Log)
				for i := 0; i < 10; i++ {
					go send(raftnode, i, logObject)
				}
			}
		}
	}
}

func send(raftnode *RaftNode, i int, logObject LogEntry) {
	if BlockNext[i] == false && (i+1) != raftnode.NodePid {
		BlockNext[i] = true
		raftnode.raftserver.Outbox() <- &cluster.Envelope{Pid: i + 1, MsgId: 0, Msg: logObject}
	}
}

func PrepareLogEntry(msg interface{}, raftnode *RaftNode) LogEntry {
	logentry.Term = raftnode.CurTerm
	logentry.LeaderId = raftnode.LdrId
	if CurrentIndex == 0 {
		logentry.PrevLogindex = 0
		logentry.PrevLogTerm = logentry.Term
	} else {
		logentry.PrevLogindex = Log[logIndex-1].Index
		logentry.PrevLogTerm = Log[logIndex-1].Term
	}
	item.Index = CurrentIndex + 1
	item.Term = logentry.Term
	item.Data = msg
	logentry.LogCommand = item
	return logentry
}

//	Handles two types of msgs, logEntry and logReply
//	if every thing goes fine then send ACK
func logEntryHandler(envelope *cluster.Envelope, raftnode *RaftNode) {
	Type := reflect.TypeOf(envelope.Msg)
	//Message is of LogEntry Type
	if Type == reflect.TypeOf(logentry) {
		raftnode.LdrId = envelope.Pid
		raftnode.TO_FLAG = 0
		logety := envelope.Msg.(LogEntry)
		x := logety.LogCommand
		//log.Println(x)
		//log.Println("Received :",logety)
		log.Println(Log)
		if (CurrentIndex == 0) && (logety.PrevLogindex == 0) {
			SendAck(x, raftnode, true)
			Log = append(Log, logety.LogCommand)
			CurrentIndex += 1
			logIndex += 1
		}

		//log.Println("@logentr recvd",CurrentIndex,logety.CommitIndex,logety.PrevLogindex,Log[logIndex-1].Index,x)
		if (CurrentIndex != 0) && (logety.PrevLogindex == Log[logIndex-1].Index) && (logety.PrevLogTerm == Log[logIndex-1].Term) {
			SendAck(x, raftnode, true)
			raftnode.CurTerm = Log[logIndex-1].Term
			Log = append(Log, logety.LogCommand)
			CurrentIndex += 1
			logIndex += 1
			var item LogItem
			y := Log[logIndex-2]
			item.Index = y.Index
			item.Term = y.Term
			item.Data = y.Data
			//update Committed data of follower nodes
			//if Log[logentry.CommitIndex-1]!=0 {return;}   //make sure that same entry isn't applied twice.
			GetAck(item, raftnode)
		} else {
			if (CurrentIndex != 0) && (logety.PrevLogindex > Log[logIndex-1].Index) && (x.Index <= logety.CommitIndex) && (logety.PrevLogindex <= logety.CommitIndex) { //???????????
				Log = append(Log, logety.LogCommand)
				CurrentIndex += 1
				logIndex += 1
				//fmt.Print(Log[logIndex-1].Index,logety.CommitIndex)
				var item LogItem
				y := Log[logIndex-1] //???? [logety.CommitIndex-1]
				item.Index = y.Index
				item.Term = y.Term
				item.Data = y.Data
				//update Committed data of follower nodes
				GetAck(item, raftnode)
				return
			}

			if (CurrentIndex != 0) && (logety.PrevLogindex != 0) {
				SendAck(x, raftnode, false) // -ve Ack
			}
		}
	}

	//Message is of LogAck Type
	if Type == reflect.TypeOf(logack) {
		logreply := envelope.Msg.(LogReply)
		BlockNext[(envelope.Pid - 1)] = false
		//fmt.Println("Ack :",envelope)
		if logreply.Reply == true {
			response := ACK[logreply.Index]
			response.positive = response.positive + 1
			ACK[logreply.Index] = response
			if response.positive == (Majority - 1) {
				//Majority has replicated the log Send Success to KVSTORE
				var item LogItem
				y := logreply.Data.(LogItem)
				item.Index = y.Index
				item.Term = y.Term
				item.Data = y.Data
				GetAck(item, raftnode)
			}
		}
		if logreply.Reply == false {
			response := ACK[logreply.Index]
			sync(envelope.Pid, logreply.Isat, raftnode)
			response.negative = response.negative + 1
			ACK[logreply.Index] = response
			if response.negative == Majority {
				logIndex -= 1
				CurrentIndex -= 1
			}
		}
	}
}

func sync(id int, index int64, raftnode *RaftNode) {
	//log.Println("Poor guy",id,"has entry upto",index)
	File, err := os.Open(logFile)
	if err != nil {
		log.Println("Error:", err)
	}
	scanner := bufio.NewScanner(File)
	var logItemBytes []byte
	var log LogItem
	var preLog LogItem
	logCount := 0
	for scanner.Scan() {
		logItemBytes = []byte(scanner.Text())
		logCount++
		json.Unmarshal([]byte(logItemBytes), &log)
		if log.Index > index {
			z := PrepareSync(preLog, log, raftnode, int64(logCount))
			//fmt.Print(id,z)
			raftnode.raftserver.Outbox() <- &cluster.Envelope{Pid: id, MsgId: 0, Msg: z}
		} else {
			preLog = log
		}
	}
	File.Close()
	if logIndex >= 1 {
		z := PrepareSync(log, Log[logIndex-1], raftnode, logIndex)
		raftnode.raftserver.Outbox() <- &cluster.Envelope{Pid: id, MsgId: 0, Msg: z}
	}
}

func PrepareSync(preLog LogItem, log LogItem, raftnode *RaftNode, committed int64) LogEntry {
	logentry.CommitIndex = committed
	logentry.Term = raftnode.CurTerm
	logentry.LeaderId = raftnode.LdrId
	logentry.PrevLogindex = preLog.Index
	logentry.PrevLogTerm = preLog.Term
	logentry.LogCommand = log
	return logentry
}

var logack LogReply

func SendAck(x LogItem, raftnode *RaftNode, reply bool) {
	if logIndex != 0 {
		logack.Isat = Log[logIndex-1].Index
	}
	logack.Index = x.Index
	logack.Reply = reply
	logack.Data = x
	//	log.Println("@ Send Ack :",raftnode.LdrId)
	if (raftnode.LdrId >= 1) && (raftnode.LdrId <= 10) {
		raftnode.raftserver.Outbox() <- &cluster.Envelope{Pid: raftnode.LdrId, MsgId: 0, Msg: logack}
	}
}
