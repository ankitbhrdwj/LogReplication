Raft Algorithm Implementation
=

What is this all about ?
-
In the cluster we need some agreement between nodes in a cluster ex: how they will communicate ?, how data will be replicated on the various nodes in that cluster etc.Raft is a consensus algorithm for clusters (agreed upon state across distributed processes even in the presence of failures )which addresses above written problems. 

How this package is useful ?
-
This package is for Election in the cluster to choose the Leader.This leader will act as a master and other nodes will be a mirror of this node.There should be a single leader at a time which will replicate the data on remaining nodes in the cluster.This package can provide service to some user level application.

Implementation Specifications
-
All the specification are from "In Search of an Understandable Consensus Algorithm" paper.This package is build in Go lang and ZMQv4 is used for communication at socket level.

How to use it ?
-
This package is giving some APIs which we can use in some other program by importing this package in that program.
  
    replicator := raft.New(myid, configfile)

This will make an object of RaftNode type which will participate in the election after an intialization.

There are some other functions which can be used for other purposes.
     
    func Leader() int {
        return LeaderId
      }

    func (node RaftNode) Term() int {
      return node.CurTerm
    }

    func (node RaftNode) isLeader() bool {
      return node.Isldr
    }
  
Who am I ?
-
I am Ankit Bhardwaj,a 1st year M.tech student at IIT Bombay,Mumbai and implementation of this projet is a part of CS 733 "Engineering a Cloud"[Spring 2014] course offered by Dr. Sriram Srinivasan.
