// Package kvslib provides an API which is a wrapper around RPC calls to the
// frontend.
package kvslib

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"sync"

	"github.com/DistributedClocks/tracing"
)

type KvslibBegin struct {
	ClientId string
}

type KvslibPut struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type KvslibPutArgs struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
	Token    tracing.TracingToken
}

type KvslibGet struct {
	ClientId string
	OpId     uint32
	Key      string
}

type KvslibGetArgs struct {
	ClientId string
	OpId     uint32
	Key      string
	Token    tracing.TracingToken
}

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
type KvslibComplete struct {
	ClientId string
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan ResultStruct
type ResultStruct struct {
	OpId        uint32
	StorageFail bool
	Result      *string
}

type KVS struct {
	frontEnd    *rpc.Client
	globalTrace *tracing.Trace
	notifyCh    NotifyChannel
	closeWg     *sync.WaitGroup
	opId        uint32
	mu sync.Mutex
	// Add more KVS instance state here.
}

func NewKVS() *KVS {
	return &KVS{
		notifyCh: nil,
		frontEnd: nil,
		opId:     0,
	}
}

// Initialize Initializes the instance of KVS to use for connecting to the frontend,
// and the frontends IP:port. The returned notify-channel channel must
// have capacity ChCapacity and must be used by kvslib to deliver all solution
// notifications. If there is an issue with connecting, this should return
// an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Initialize(flocalTracer *tracing.Tracer, clientId string, frontEndAddr string, chCapacity uint) (NotifyChannel, error) {
	log.Printf("Dialing FrontEnd at %s", frontEndAddr)
	d.globalTrace = flocalTracer.CreateTrace()
	d.globalTrace.RecordAction(KvslibBegin{ClientId: clientId})
	frontEnd, err := rpc.Dial("tcp", frontEndAddr)
	if err != nil {
		return nil, fmt.Errorf("error dialing frontEnd: %s", err)
	}
	d.frontEnd = frontEnd

	// create notify channel with given capacity
	d.notifyCh = make(NotifyChannel, chCapacity)

	var wg sync.WaitGroup
	d.closeWg = &wg

	return d.notifyCh, nil
}

// Get is a non-blocking request from the client to the system. This call is used by
// the client when it wants to get value for a key.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	// Should return OpId or error
	log.Println("Called get.")
	trace := tracer.CreateTrace()
	d.closeWg.Add(1)
	go d.callGet(tracer, trace, clientId, key)
	return 0, nil
}

func (d *KVS) callGet(tracer *tracing.Tracer, trace *tracing.Trace, clientId string, key string) {
	defer func() {
		log.Printf("callGet done")
		d.closeWg.Done()
	}()
	d.mu.Lock()
	args := KvslibGetArgs{
		ClientId: clientId,
		OpId:     d.opId,
		Key:      key,
		Token:    nil, // Generate after recording
	}
	trace.RecordAction(KvslibGet{
		ClientId: clientId,
		OpId:     d.opId,
		Key:      key,
	})
	d.opId = d.opId + 1
	d.mu.Unlock()
	args.Token = trace.GenerateToken()
	result := KvslibGetReply{}
	call := d.frontEnd.Go("FrontEndRPCHandler.Get", args, &result, nil)
	for {
		select {
		case <-call.Done:
			if call.Error != nil {
				log.Fatal(call.Error)
			} else {
				// Handle result
				tracer.ReceiveToken(result.Token)
				trace.RecordAction(KvslibGetResult{
					OpId:  result.OpId,
					Key:   result.Key,
					Value: result.Value,
					Err:   result.Err,
				})
				d.notifyCh <- ResultStruct{
					OpId:        result.OpId,
					StorageFail: result.Err,
					Result:      result.Value,
				}
			}
			return
		}
	}
}

// Put is a non-blocking request from the client to the system. This call is used by
// the client when it wants to update the value of an existing key or add add a new
// key and value pair.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	// Should return OpId or error
	log.Println("Called put.")
	trace := tracer.CreateTrace()
	d.closeWg.Add(1)
	go d.callPut(tracer, trace, clientId, key, value)
	return 0, nil
}

func (d *KVS) callPut(tracer *tracing.Tracer, trace *tracing.Trace, clientId string, key string, value string) {
	defer func() {
		log.Printf("callPut done")
		d.closeWg.Done()
	}()
	d.mu.Lock()
	args := KvslibPutArgs{
		ClientId: clientId,
		OpId:     d.opId,
		Key:      key,
		Value:    value,
		Token:    nil,
	}
	trace.RecordAction(KvslibPut{
		ClientId: clientId,
		OpId:     d.opId,
		Key:      key,
		Value:    value,
	})
	d.opId = d.opId + 1
	d.mu.Unlock()
	args.Token = trace.GenerateToken()
	result := KvslibPutReply{}
	call := d.frontEnd.Go("FrontEndRPCHandler.Put", args, &result, nil)
	for {
		select {
		case <-call.Done:
			if call.Error != nil {
				log.Fatal(call.Error)
			} else {
				// Handle result
				tracer.ReceiveToken(result.Token)
				trace.RecordAction(KvslibPutResult{
					OpId: result.OpId,
					Err:  result.Err,
				})
				d.notifyCh <- ResultStruct{
					OpId:        result.OpId,
					StorageFail: result.Err,
					Result:      nil,
				}
			}
			return
		}
	}
}

// Close Stops the KVS instance from communicating with the frontend and
// from delivering any solutions via the notify-channel. If there is an issue
// with stopping, this should return an appropriate err value, otherwise err
// should be set to nil.
func (d *KVS) Close() error {
	d.globalTrace.RecordAction(KvslibComplete{})
	return errors.New("not implemented")
}
