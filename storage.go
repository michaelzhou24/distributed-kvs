package distkvs

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"log"
	"net"
	"net/rpc"
	"os"
)

type StorageConfig struct {
	StorageID        string
	StorageAdd       StorageAddr
	ListenAddr       string
	FrontEndAddr     string
	DiskPath         string
	TracerServerAddr string
	TracerSecret     []byte
}

type StorageLoadSuccess struct {
	State map[string]string
}

type StoragePut struct {
	Key   string
	Value string
}

type StorageSaveData struct {
	Key   string
	Value string
}

type StorageGet struct {
	Key string
}

type StorageGetResult struct {
	Key   string
	Value string
}

type Storage struct {
	// state may go here
	tracer         *tracing.Tracer
	frontEndClient *rpc.Client
	memoryKVS      map[string]string
	diskFile       *os.File
	diskPath       string
}

// FrontEndAddr - IP:Port of frontend node to connect to
func (s *Storage) Start(frontEndAddr string, storageAddr string, diskPath string, trace *tracing.Tracer) error {
	// Connect to frontEND
	s.tracer = trace
	log.Printf("dailing frontEnd at %s", frontEndAddr)
	frontEnd, err := rpc.Dial("tcp", frontEndAddr)
	if err != nil {
		return fmt.Errorf("error dialing coordinator: %s", err)
	}
	s.frontEndClient = frontEnd
	//
	//// Listen or something?
	server := rpc.NewServer()
	frontEndListener, e := net.Listen("tcp", storageAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", storageAddr, e)
	}
	server.Accept(frontEndListener)
	s.diskPath = diskPath
	s.memoryKVS = make(map[string]string)
	// Check if file on disk exists
	if _, err := os.Stat(diskPath); err == nil {
		fmt.Printf("Disk path exists \n")
		// path/to/whatever exists
		// If disk contains some stuff, restore state to memory; (map)
		s.diskFile, err = os.OpenFile(diskPath, os.O_APPEND|os.O_RDWR, 0600)
		if err != nil {
			panic(err)
		}
		decoder := gob.NewDecoder(s.diskFile)
		err := decoder.Decode(&s.memoryKVS)
		if err != nil {
			fmt.Print("Error decoding map from disk at start! \n")
			//	panic(err)
		}

	} else if os.IsNotExist(err) {
		// path/to/whatever does *not* exist
		fmt.Printf("Disk path does not exist! \n")
		disk, err := os.OpenFile(diskPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)

		//disk, err := os.Create("/" + diskPath)
		if err != nil {
			panic(err)
		}
		s.diskFile = disk

	}

	return nil
}
func (s *Storage) Get(args StorageGet, reply *FrontEndGetResult) error {
	// trace.recievetoken(args.token)
	key := args.Key

	val, err := s.memoryKVS[key]
	if err == false {
		log.Printf("Key %s not in map!\n", key)
		reply.Key = args.Key
		reply.Value = nil
		reply.Err = true
		// reply.traceToken = trace.gen
		return errors.New("ket not in map")
	}
	log.Printf("Hit for map; %s:%s \n", key, val)

	reply.Key = args.Key
	reply.Value = &val
	reply.Err = false
	// reply.traceToken = trace.gen
	return nil
}

func (s *Storage) Put(args StoragePut, reply *FrontEndPutResult) error {
	// trace := s.tracer.RecieveTokren(args.TraceToken)
	key := args.Key
	value := args.Value
	log.Printf("Writing to disk; %s:%s...  \n", key, value)

	err := errors.New("")
	//Probably inefficient. We read file again and reset its contents before writing the whole map again
	if err := s.diskFile.Close(); err != nil {
		panic(err)
	}
	s.diskFile, err = os.OpenFile(s.diskPath, os.O_TRUNC|os.O_RDWR|os.O_CREATE, 0600)

	if err != nil {
		log.Printf("Truncate reset error!")
		panic(err)
	}
	s.memoryKVS[key] = value
	encoder := gob.NewEncoder(s.diskFile)
	if err := encoder.Encode(s.memoryKVS); err != nil {
		panic(err)
	}

	// reply.tracetoken = trace.generateToken()
	reply.Err = false
	return nil
}

func (s *Storage) Close() {
	log.Printf("Closing storage node...\n")
	if err := s.diskFile.Close(); err != nil {
		panic(err)
	}

}

func (s *Storage) TestSuite() {

	//s.Start(".", ".", "diskFile.txt", nil)
	//s.Get(nil, "testKey1") // should be empty
	//s.Put(nil, "testKey1", "testVal1")
	//s.Close(nil)
	//
	//s.Start(".", ".", "diskFile.txt", nil)
	//s.Get(nil, "testKey1")           // should get testVal1
	//s.Put(nil, "testKey1", "NewVAL") // should write
	//s.Close(nil)
	//
	//s.Start(".", ".", "diskFile.txt", nil)
	//s.Get(nil, "testKey1") // should get NEWVAL
	//s.Close(nil)

}
