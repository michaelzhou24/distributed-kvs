package main

import (
	"flag"
	"log"

	distkvs "example.org/cpsc416/a5"
	"example.org/cpsc416/a5/kvslib"
)

func main() {
	var config distkvs.ClientConfig
	err := distkvs.ReadJSONConfig("config/client_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&config.ClientID, "id", config.ClientID, "Client ID, e.g. client1")
	flag.Parse()

	client := distkvs.NewClient(config, kvslib.NewKVS())
	if err := client.Initialize(); err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err, _ := client.Put("clientID1", "key2", "value1"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Put("clientID1", "key2", "value2"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Put("clientID1", "key2", "value3"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Put("clientID1", "key2", "value4"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Put("clientID1", "key2", "value5"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Put("clientID1", "key2", "value6"); err != 0 {
		log.Println(err)
	}

	for i := 0; i < 6; i++ {
		result := <-client.NotifyChannel
		log.Println(result)
	}

	if err, _ := client.Get("clientID1", "key2"); err != 0 {
		log.Println(err)
	}

	for i := 0; i < 1; i++ {
		result := <-client.NotifyChannel
		log.Println(result)
	}
}
