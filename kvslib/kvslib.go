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
	Token tracing.TracingToken
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
	Token tracing.TracingToken
}

type KvslibPutResult struct {
	OpId uint32
	Err  bool
}

type KvslibGetResult struct {
	OpId  uint32
	Key   string
	Value string
	Err   bool
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
	frontEnd *rpc.Client
	notifyCh    NotifyChannel
	closeWg     *sync.WaitGroup
	opId uint32
	// Add more KVS instance state here.
}

func NewKVS() *KVS {
	return &KVS{
		notifyCh: nil,
		frontEnd: nil,
		opId: 0,
	}
}

// Initialize Initializes the instance of KVS to use for connecting to the frontend,
// and the frontends IP:port. The returned notify-channel channel must
// have capacity ChCapacity and must be used by kvslib to deliver all solution
// notifications. If there is an issue with connecting, this should return
// an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Initialize(frontEndAddr string, chCapacity uint) (NotifyChannel, error) {
	log.Printf("Dialing FrontEnd at %s", frontEndAddr)
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
	trace := tracer.CreateTrace()
	d.closeWg.Add(1)
	go d.callGet(tracer, trace, clientId, key)
	return 0, errors.New("not implemented")
}

func (d *KVS) callGet(tracer *tracing.Tracer, trace *tracing.Trace, clientId string, key string) {
	defer func() {
		log.Printf("callGet done")
		d.closeWg.Done()
	}()
	args := KvslibGetArgs{
		ClientId: clientId,
		OpId:     0,
		Key:      key,
		Token: trace.GenerateToken(),
	}
	trace.RecordAction(KvslibGet{
		ClientId: clientId,
		OpId:     0,
		Key:      key,
	})
	result := ResultStruct{}
	call := d.frontEnd.Go("FrontEnd.Get", args, &result, nil)
	for {
		select {
		case <-call.Done:
			if call.Error != nil {
				log.Fatal(call.Error)
			} else {
				// Handle result
				d.notifyCh <- result
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
	trace := tracer.CreateTrace()
	d.closeWg.Add(1)
	go d.callPut(tracer, trace, clientId, key, value)
	return 0, errors.New("not implemented")
}

func (d *KVS) callPut(tracer *tracing.Tracer, trace *tracing.Trace, clientId string, key string, value string) {
	defer func() {
		log.Printf("callGet done")
		d.closeWg.Done()
	}()
	args := KvslibPutArgs{
		ClientId: clientId,
		OpId:     0,
		Key:      key,
		Value: value,
		Token: trace.GenerateToken(),
	}
	trace.RecordAction(KvslibPut{
		ClientId: clientId,
		OpId:     0,
		Key:      key,
		Value: value,
	})
	result := ResultStruct{}
	call := d.frontEnd.Go("FrontEnd.Put", args, &result, nil)
	for {
		select {
		case <-call.Done:
			if call.Error != nil {
				log.Fatal(call.Error)
			} else {
				// Handle result
				d.notifyCh <- result
			}
			return
		}
	}
}

// Close Stops the KVS instance from communicating with the frontend and
// from delivering any solutions via the notify-channel. If there is an issue
// with stopping, this should return an appropriate err value, otherwise err
// should be set to nil.
func (d *KVS) Close(tracer *tracing.Tracer) error {
	return errors.New("not implemented")
}
