package distkvs

import (
	"errors"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"log"
	"math"
	"net"
	"net/rpc"
	"sync"
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

type FrontEndStorageStarted struct {
	StorageID string
}

type FrontEndStorageFailed struct {
	StorageID string
}

type FrontEndPut struct {
	Key   string
	Value string
}

type FrontEndPutResult struct {
	Err bool
}

type FrontEndPutReply struct {
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
}
type FrontEndGetReply struct {
	Key   string
	Value *string
	Err   bool
	Token tracing.TracingToken
}

type FrontEndStorageJoined struct {
	StorageIds []string
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
	StorageID   string
	Token       tracing.TracingToken
}

type FrontEndConnectReply struct {
	Token tracing.TracingToken
}
type FrontEndReqStateArgs struct {
	Token tracing.TracingToken
}
type FrontEndReqStateReply struct {
	Token tracing.TracingToken
	State map[string]string
}
type FrontEndReqJoinArgs struct {
	Token     tracing.TracingToken
	StorageID string
}
type FrontEndReqJoinReply struct {
	Token tracing.TracingToken
}
type FrontEnd struct {
	// FrontEnd state
}

type OpIdChan chan uint32
type StorageNode struct {
	client *rpc.Client
	joined bool
}
type FrontEndRPCHandler struct {
	StorageTimeout uint8
	Tracer         *tracing.Tracer
	Storages       map[string]*rpc.Client
	ClientState    map[string]OpIdChan
	JoinedStorages map[string]bool // this is a set; need a way to differentiate joined nodes and connected nodes
	Mutex          sync.Mutex
}

// API Endpoint for storage to connect to when it starts up
func (f *FrontEndRPCHandler) Connect(args FrontEndConnectArgs, reply *FrontEndConnectReply) error {
	trace := f.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(FrontEndStorageStarted{args.StorageID})
	log.Println("frontend: Dialing storage....")
	storage, err := rpc.Dial("tcp", args.StorageAddr)
	if err != nil {
		log.Printf("frontend: Error dialing storage node from front end \n")
		return fmt.Errorf("failed to dial storage: %s", err)
	}
	f.Storages[args.StorageID] = storage

	if f.Storages[args.StorageID] == nil {
		log.Printf("frontEnd: Storage ref in front is nil! \n")
		delete(f.Storages, args.StorageID)
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
		ClientState:    make(map[string]OpIdChan),
		Storages:       make(map[string]*rpc.Client),
		JoinedStorages: make(map[string]bool, 256),
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

func (f *FrontEndRPCHandler) addToClientMap(clientId string) {
	if _, ok := f.ClientState[clientId]; !ok {
		f.ClientState[clientId] = make(OpIdChan, math.MaxUint8)
		f.ClientState[clientId] <- 0
	}
}

func (f *FrontEndRPCHandler) Put(args FrontEndPutArgs, reply *KvslibPutReply) error {
	trace := f.Tracer.ReceiveToken(args.Token)
	f.addToClientMap(args.ClientId)
waiting:
	for {
		select {
		case val := <-f.ClientState[args.ClientId]:
			if val >= args.OpId {
				log.Println(val)
				break waiting
			} else {
				f.ClientState[args.ClientId] <- val
			}
		}
	}
	trace.RecordAction(FrontEndPut{
		Key:   args.Key,
		Value: args.Value,
	})
	callArgs := StoragePutArgs{Key: args.Key, Value: args.Value, Token: trace.GenerateToken()}
	currentActiveStorages := len(f.JoinedStorages)
	log.Printf("\n \n ############ Joined storages at put is: %s \n\n", f.JoinedStorages)
	replies := make([]FrontEndPutReply, currentActiveStorages)
	index := 0
	//mu := sync.Mutex{}
	storageCalls := make(chan uint8, currentActiveStorages)
	for storageID, element := range f.Storages {
		if !f.JoinedStorages[storageID] {
			continue
		}
		go func(localIdx int, element *rpc.Client) {
			//mu.Lock()
			//localIdx := index
			//index = index + 1
			//mu.Unlock()
			log.Println(localIdx)
			err := f.callPut(callArgs, &(replies[localIdx]), element, 1)
			if err != nil {
				trace.RecordAction(FrontEndStorageFailed{StorageID: storageID})
				delete(f.Storages, storageID)
				delete(f.JoinedStorages, storageID)
				trace.RecordAction(FrontEndStorageJoined{f.getJoinedStorageIDs()})
			}
			storageCalls <- 1
		}(index, element)
		index = index + 1
	}
	for i := 0; i < currentActiveStorages; i++ {
		<-storageCalls
	}

	// TODO: handle replies, handle Err
	putReply := replies[0]
	f.ClientState[args.ClientId] <- (args.OpId + 1)

	reply.OpId = args.OpId
	f.Tracer.ReceiveToken(putReply.Token)
	trace.RecordAction(FrontEndPutResult{Err: putReply.Err})
	reply.Token = trace.GenerateToken() // for kvslib

	return nil
}

func (f *FrontEndRPCHandler) callPut(callArgs StoragePutArgs, putReply *FrontEndPutReply,
	storageClient *rpc.Client, retry uint8) error {

	c := make(chan error, 1)
	go func() { c <- storageClient.Call("StorageRPC.Put", callArgs, putReply) }()
	select {
	case err := <-c:
		// use err and result
		if err == rpc.ErrShutdown {
			log.Printf("Storage connection shutdown, retrying... \n")

			time.Sleep(time.Duration(f.StorageTimeout) * time.Second)
			if retry == 1 {
				return f.callPut(callArgs, putReply, storageClient, 0)
			} else {
				log.Println("timed out after retrying")
				return errors.New("timed out after retrying")
			}
		}

		log.Println("Done with callput", putReply)
		return err
	case <-time.After(time.Duration(uint64(f.StorageTimeout) * 1e9)):
		// call timed out
		if retry == 1 {
			return f.callPut(callArgs, putReply, storageClient, 0)
		} else {
			log.Println("timed out after retrying")
			return errors.New("timed out after retrying")
		}
	}
}
func (f *FrontEndRPCHandler) Join(args FrontEndReqJoinArgs, reply *FrontEndReqJoinReply) error {
	f.Mutex.Lock()
	trace := f.Tracer.ReceiveToken(args.Token)
	f.JoinedStorages[args.StorageID] = true

	trace.RecordAction(FrontEndStorageJoined{StorageIds: f.getJoinedStorageIDs()})
	reply.Token = trace.GenerateToken()
	f.Mutex.Unlock()
	return nil
}

func (f *FrontEndRPCHandler) RequestState(args FrontEndReqStateArgs, reply *FrontEndReqStateReply) error {
	trace := f.Tracer.ReceiveToken(args.Token)

	if len(f.JoinedStorages) == 0 {

		reply.Token = trace.GenerateToken()
		reply.State = make(map[string]string, 0)
		return nil
	}
	// Finds up to date storage node;

	for k := range f.JoinedStorages {
		c := f.Storages[k] // Returns random joined client;
		reqStateArgs := StorageGetStateArgs{Token: trace.GenerateToken()}
		reqStateReply := StorageGetStateReply{}
		e := c.Call("StorageRPC.GetState", reqStateArgs, &reqStateReply) // TODO: blocking or non blocking??
		trace = f.Tracer.ReceiveToken(reqStateReply.Token)
		if e != nil {
			log.Printf("error calling getstate to node %s, retrying again...\n", k)
			time.Sleep(time.Duration(f.StorageTimeout) * time.Second)
			reqStateArgs := StorageGetStateArgs{Token: trace.GenerateToken()}
			reqStateReply := StorageGetStateReply{}
			e = c.Call("StorageRPC.GetState", reqStateArgs, &reqStateReply)
			if e != nil {
				trace.RecordAction(FrontEndStorageFailed{StorageID: k})
				delete(f.Storages, k)
				delete(f.JoinedStorages, k)
				trace.RecordAction(FrontEndStorageJoined{f.getJoinedStorageIDs()})
				log.Println("Error calling GetState(); retrying with another node...")
			}
		} else {
			reply.Token = trace.GenerateToken()
			reply.State = reqStateReply.State
			return nil
		}
	}
	log.Printf("Could not connect to any node to update state! \n")
	reply.Token = trace.GenerateToken()
	reply.State = make(map[string]string, 0)
	// RPC call to that node with getState() --> map; non blocking
	return nil
}
func (f *FrontEndRPCHandler) getJoinedStorageIDs() []string {
	keys := make([]string, 0, len(f.JoinedStorages))
	for k := range f.JoinedStorages {
		keys = append(keys, k)
	}
	return keys
}
func (f *FrontEndRPCHandler) Get(args FrontEndGetArgs, reply *KvslibGetReply) error {
	trace := f.Tracer.ReceiveToken(args.Token)
	f.addToClientMap(args.ClientId)
waiting:
	for {
		select {
		case val := <-f.ClientState[args.ClientId]:
			if val >= args.OpId {
				break waiting
			} else {
				f.ClientState[args.ClientId] <- val
			}
		}
	}
	trace.RecordAction(FrontEndGet{
		Key: args.Key,
	})
	callArgs := StorageGetArgs{Key: args.Key, Token: trace.GenerateToken()}
	currentActiveStorages := len(f.JoinedStorages)
	replies := make([]FrontEndGetReply, currentActiveStorages)
	index := 0
	//mu := sync.Mutex{}
	storageCalls := make(chan uint8, currentActiveStorages)
	for storageID, element := range f.Storages {
		if !f.JoinedStorages[storageID] {
			continue
		}
		go func(localIdx int, element *rpc.Client) {
			//mu.Lock()
			//localIdx := index
			//index = index + 1
			//mu.Unlock()
			log.Println(localIdx)
			err := f.callGet(callArgs, &(replies[localIdx]), element, 1)
			if err != nil {
				trace.RecordAction(FrontEndStorageFailed{StorageID: storageID})
				delete(f.Storages, storageID)
				delete(f.JoinedStorages, storageID)
				trace.RecordAction(FrontEndStorageJoined{f.getJoinedStorageIDs()})
			}
			storageCalls <- 1
		}(index, element)
		index = index + 1
	}
	for i := 0; i < currentActiveStorages; i++ {
		<-storageCalls
	}
	f.ClientState[args.ClientId] <- (args.OpId + 1)
	// TODO: Handle replies
	getReply := replies[0]
	f.Tracer.ReceiveToken(getReply.Token)
	trace.RecordAction(FrontEndGetResult{
		Key:   args.Key,
		Value: getReply.Value,
		Err:   getReply.Err,
	})

	reply.Token = trace.GenerateToken()
	reply.Value = getReply.Value
	reply.Key = args.Key
	reply.OpId = args.OpId

	return nil
}

func (f *FrontEndRPCHandler) callGet(callArgs StorageGetArgs, getReply *FrontEndGetReply, storageClient *rpc.Client, retry uint8) error {
	c := make(chan error, 1)
	go func() { c <- storageClient.Call("StorageRPC.Get", callArgs, getReply) }()
	select {
	case err := <-c:
		if err == rpc.ErrShutdown {
			log.Printf("Storage connection shutdown, retrying... \n")
			time.Sleep(time.Duration(f.StorageTimeout) * time.Second)
			if retry == 1 {
				return f.callGet(callArgs, getReply, storageClient, 0)
			} else {
				log.Println("timed out after retrying")
				return errors.New("timed out after retrying")
			}
		}
		log.Println("Done with callget", getReply)
		return err
		// use err and result
	case <-time.After(time.Duration(uint64(f.StorageTimeout) * 1e9)):
		// call timed out
		if retry == 1 {
			return f.callGet(callArgs, getReply, storageClient, 0)
		} else {
			log.Println("timed out after retrying")
			return errors.New("timed out after retrying")
		}
	}
}
