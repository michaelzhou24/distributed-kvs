package main

import (
	"flag"
	"log"

	distkvs "example.org/cpsc416/a5"
	"github.com/DistributedClocks/tracing"
)

func main() {
	var config distkvs.StorageConfig
	var configFile string
	flag.StringVar(&configFile, "config", "config/storage_config.json", "Config")
	flag.Parse()
	err := distkvs.ReadJSONConfig(configFile, &config)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(config)

	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: config.StorageID,
		Secret:         config.TracerSecret,
	})

	storage := distkvs.Storage{}
	err = storage.Start(config.StorageID, config.FrontEndAddr, string(config.StorageAdd), config.DiskPath, tracer)
	if err != nil {
		log.Fatal(err)
	}
	select {}
}
