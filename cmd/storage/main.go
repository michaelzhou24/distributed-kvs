package main

import (
	//"github.com/DistributedClocks/tracing"
	"log"

	distkvs "example.org/cpsc416/a5"
	//"github.com/DistributedClocks/tracing"
)

func main() {
	var config distkvs.StorageConfig
	err := distkvs.ReadJSONConfig("config/storage_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(config)

	//tracer := tracing.NewTracer(tracing.TracerConfig{
	//	ServerAddress:  config.TracerServerAddr,
	//	TracerIdentity: "storage",
	//	Secret:         config.TracerSecret,
	//})

	storage := distkvs.Storage{}
	err = storage.Start(config.FrontEndAddr, string(config.StorageAdd), "testDiskFile.txt", nil)
	if err != nil {
		log.Fatal(err)
	}
}
