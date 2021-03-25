package distkvs

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/DistributedClocks/tracing"
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
	frontEndClient *rpc.Client
	memoryKVS      map[string]string
	diskFile       *os.File
}

// FrontEndAddr - IP:Port of frontend node to connect to
func (s *Storage) Start(frontEndAddr string, storageAddr string, diskPath string, strace *tracing.Tracer) error {
	// Connect to frontEND
	//log.Printf("dailing frontEnd at %s", frontEndAddr)
	//frontEnd, err := rpc.Dial("tcp", frontEndAddr)
	//if err != nil {
	//	return fmt.Errorf("error dialing coordinator: %s", err)
	//}
	//s.frontEndClient = frontEnd
	//
	//// Listen or something?
	//server := rpc.NewServer()
	//frontEndListener, e := net.Listen("tcp", storageAddr)
	//if e != nil {
	//	return fmt.Errorf("failed to listen on %s: %s", storageAddr, e)
	//}
	//server.Accept(frontEndListener)

	// Check if file on disk exists
	if _, err := os.Stat(diskPath); err == nil {
		fmt.Printf("Disk path exists! \n")
		// path/to/whatever exists
		// If disk contains some stuff, restore state to memory; (map)
		s.diskFile, err = os.Open(diskPath)
		if err != nil {
			fmt.Println(err)
		}
		decoder := gob.NewDecoder(s.diskFile)
		err := decoder.Decode(&s.memoryKVS)
		if err != nil {
			fmt.Print("Error decoding map from disk at start! \n")
		}

	} else if os.IsNotExist(err) {
		// path/to/whatever does *not* exist
		fmt.Printf("Disk path does not exist! \n")
		disk, err := os.OpenFile(diskPath, os.O_RDONLY|os.O_CREATE, 0666)

		//disk, err := os.Create("/" + diskPath)
		if err != nil {
			panic(err)
		}
		s.diskFile = disk

	}

	return nil
}
func (*Storage) Get(tracer *tracing.Tracer, clientId string, key string) (int, error) {
	return -1, errors.New("not implemented")
}

func (*Storage) Put(tracer *tracing.Tracer, clientId string, key string, value string) (int, error) {
	return -1, errors.New("not implemented")

}

func (*Storage) Close(tracer *tracing.Tracer) {

}

func testSuite() {
	s := Storage{
		frontEndClient: nil,
		memoryKVS:      nil,
		diskFile:       nil,
	}
	s.Start(".", ".", "diskFile.txt", nil)

}
