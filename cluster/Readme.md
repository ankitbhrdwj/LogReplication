About Cluster
============
This directory contains the programs written in Go language which is used to send and receive the messages in a cluster.
Various nodes in a cluster can send and receive messages using the package cluster.This package is written in Go language and can be imported in any other go program to use this kind of service in the program.

What is this all about ?
=
This package is giving the basic functionality to send and receive messages in the cluster.This package can be used to provide Communication service to use for some other purpose.for example :leader election within the cluster.

Implementation Specifications
=
This package is written in the GO language and I have used ZMQv4 for socket level implementation.

Config.txt
-
This file contains the information related to other peers in the cluster.Basically each peers in the cluster should know about every other peers in that cluster.
Format which i have used in this file is like this {PeerID,IP:PORT} ex 1,127.0.0.1:8001

Cluster_test.go
-
This is the test file for cluster package.This is used to test the program.Basically test cases are meant to check the robustness of the program.But i think there is lot of scope to improve the testcases which i have used.

Cluster.go
-
    const (BROADCAST = -1)
    type Envelope struct {
        Pid int
        MsgId int64
        Msg interface{}
    }
   
    type Server interface {
        Pids() int
        Peers() []int
        Outbox() chan *Envelope
        Inbox() chan *Envelope
    } 

Functions and variables which can be used after importing the package .

Example:
-
//example.go

    package main
    import ("github.com/ankitbhrdwj27/cluster")
    func main() {
    var myid int
    server := cluster.New(myid, /* config file */"config.json")
    server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, Msg: "hello there"}
    select {
           case envelope := <- case server.Inbox(): 
               fmt.Printf("Received msg from %d: '%s'\n", envelope.Pid, envelope.Msg)

           case <- time.After(10 * time.Second): 
           println("Waited and waited. Ab thak gaya\n")
       }
    }

Who am I ?
=
I am Ankit Bhardwaj,a 1st year M.tech student at IIT Bombay,Mumbai and implementation of this projet is a part of CS 733 "Engineering a Cloud"[Spring 2014] course offered by Dr. Sriram Srinivasan.
