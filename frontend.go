package distkvs

import (
	"errors"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"log"
	"net"
	"net/rpc"
	"time"
)

type StorageAddr string

// this matches the config file format in config/frontend_config.json
type FrontEndConfig struct {
	ClientAPIListenAddr  string
	StorageAPIListenAddr string
	Storage              StorageAddr
	TracerServerAddr     string
	TracerSecret         []byte
}

type FrontEndStorageStarted struct{}

type FrontEndStorageFailed struct{}

type FrontEndPut struct {
	Key   string
	Value string
	Token tracing.TracingToken
}

type FrontEndPutResult struct {
	Err   bool
	Token tracing.TracingToken
}

type FrontEndGet struct {
	Key string
}

type FrontEndGetResult struct {
	Key   string
	Value *string
	Err   bool
	Token tracing.TracingToken
}

// KVS Reply structs:
type KvslibPutResult struct {
	OpId uint32
	Err  bool
}
type KvslibPutReply struct {
	OpId  uint32
	Err   bool
	Token tracing.TracingToken
}
type KvslibGetResult struct {
	OpId  uint32
	Key   string
	Value *string
	Err   bool
}
type KvslibGetReply struct {
	OpId  uint32
	Key   string
	Value *string
	Err   bool
	Token tracing.TracingToken
}

// RPC Structs Below:
type FrontEndPutArgs struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
	Token    tracing.TracingToken
}

type FrontEndGetArgs struct {
	ClientId string
	OpId     uint32
	Key      string
	Token    tracing.TracingToken
}

type FrontEndConnectArgs struct {
	StorageAddr string
	Token       tracing.TracingToken
}

type FrontEndConnectReply struct {
	Token tracing.TracingToken
}

type FrontEnd struct {
	// FrontEnd state
}

type FrontEndRPCHandler struct {
	StorageTimeout uint8
	Tracer         *tracing.Tracer
	Storage        *rpc.Client
}

// API Endpoint for storage to connect to when it starts up
func (f *FrontEndRPCHandler) Connect(args FrontEndConnectArgs, reply *FrontEndConnectReply) error {
	trace := f.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(FrontEndStorageStarted{})
	log.Println("frontend: Dialing storage....")
	storage, err := rpc.Dial("tcp", args.StorageAddr)
	if err != nil {
		log.Printf("frontend: Error dialing storage node from front end \n")
		return fmt.Errorf("failed to dial storage: %s", err)
	}
	f.Storage = storage
	if f.Storage == nil {
		log.Printf("frontEnd: Storage ref in front is nil! \n")
	} else {
		log.Printf("frontEnd: Succesfully connected to storage node! \n")
	}
	reply.Token = trace.GenerateToken()

	return nil
}

func (f *FrontEnd) Start(clientAPIListenAddr string, storageAPIListenAddr string, storageTimeout uint8, ftrace *tracing.Tracer) error {
	handler := FrontEndRPCHandler{
		StorageTimeout: storageTimeout,
		Tracer:         ftrace,
	}
	server := rpc.NewServer()
	err := server.Register(&handler)
	if err != nil {
		return fmt.Errorf("format of FrontEnd RPCs aren't correct: %s", err)
	}

	storageListener, e := net.Listen("tcp", storageAPIListenAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", storageAPIListenAddr, e)
	}

	clientListener, e := net.Listen("tcp", clientAPIListenAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", clientAPIListenAddr, e)
	}

	go server.Accept(storageListener)
	server.Accept(clientListener)
	return nil
}

func (f *FrontEndRPCHandler) Put(args FrontEndPutArgs, reply *KvslibPutReply) error {
	trace := f.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(FrontEndPut{
		Key:   args.Key,
		Value: args.Value,
	})
	callArgs := StoragePutArgs{Key: args.Key, Value: args.Value, Token: trace.GenerateToken()}
	putReply := FrontEndPutResult{}

	err := f.callPut(callArgs, &putReply, 1)
	if err != nil {
		reply.Err = true
		// TODO: trace that storage is dead
		trace.RecordAction(FrontEndStorageFailed{})
	} else {
		reply.Err = putReply.Err
	}
	reply.OpId = args.OpId
	f.Tracer.ReceiveToken(putReply.Token)
	trace.RecordAction(putReply)
	reply.Token = trace.GenerateToken() // for kvslib

	return nil
}

func (f *FrontEndRPCHandler) callPut(callArgs StoragePutArgs, putReply *FrontEndPutResult, retry uint8) error {
	c := make(chan error, 1)
	go func() { c <- f.Storage.Call("StorageRPC.Put", callArgs, putReply) }()
	select {
	case err := <-c:
		// use err and result
		log.Println("Done with callput", putReply)
		return err
	case <-time.After(time.Duration(uint64(f.StorageTimeout) * 1e9)):
		// call timed out
		if retry == 1 {
			return f.callPut(callArgs, putReply, 0)
		} else {
			log.Println("timed out after retrying")
			return errors.New("timed out after retrying")
		}
	}
	return nil
}

func (f *FrontEndRPCHandler) Get(args FrontEndGetArgs, reply *KvslibGetReply) error {
	trace := f.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(FrontEndGet{
		Key: args.Key,
	})
	callArgs := StorageGetArgs{Key: args.Key, Token: trace.GenerateToken()}
	getReply := FrontEndGetResult{}
	if f.Storage == nil {
		log.Printf("Storage ref in front is nil! \n")
	}

	err := f.callGet(callArgs, &getReply, 1)
	log.Println("asdasdasdad", getReply)
	if err != nil {
		reply.Err = true

		// TODO: trace that storage is dead
		trace.RecordAction(FrontEndStorageFailed{})
	} else {
		reply.Err = getReply.Err
	}

	f.Tracer.ReceiveToken(getReply.Token)
	trace.RecordAction(getReply)

	reply.Token = trace.GenerateToken()
	reply.Value = getReply.Value
	reply.Key = getReply.Key
	reply.OpId = args.OpId

	return nil
}

func (f *FrontEndRPCHandler) callGet(callArgs StorageGetArgs, getReply *FrontEndGetResult, retry uint8) error {
	c := make(chan error, 1)
	go func() { c <- f.Storage.Call("StorageRPC.Get", callArgs, getReply) }()
	select {
	case err := <-c:
		log.Println("Done with callget", getReply)
		return err
		// use err and result
	case <-time.After(time.Duration(uint64(f.StorageTimeout) * 1e9)):
		// call timed out
		if retry == 1 {
			return f.callGet(callArgs, getReply, 0)
		} else {
			log.Println("timed out after retrying")
			return errors.New("timed out after retrying")
		}
	}
}
