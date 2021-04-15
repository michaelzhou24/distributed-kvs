package main

import (
	distkvs "example.org/cpsc416/a5"
	"example.org/cpsc416/a5/kvslib"
	"flag"
	"log"
)

func main() {
	var config distkvs.ClientConfig
	err := distkvs.ReadJSONConfig("config/client_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&config.ClientID, "id", config.ClientID, "Client ID, e.g. client1")
	flag.Parse()

	var config2 distkvs.ClientConfig
	err = distkvs.ReadJSONConfig("config/client2_config.json", &config2)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&config2.ClientID, "id2", config2.ClientID, "Client ID, e.g. client1")
	flag.Parse()

	client := distkvs.NewClient(config, kvslib.NewKVS())
	if err := client.Initialize(); err != nil {
		log.Fatal(err)
	}
	client2 := distkvs.NewClient(config2, kvslib.NewKVS())
	if err := client2.Initialize(); err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	defer client2.Close()
	if err, _ := client.Put(config.ClientID, "k", "v1"); err != 0 {
		log.Println(err)
	}
	//time.Sleep(3*time.Second)
	if err, _ := client2.Put("client2", "k", "v2"); err != 0 {
		log.Println(err)
	}

	if err, _ := client.Get(config.ClientID, "k"); err != 0 {
		log.Println(err)
	}
	//time.Sleep(3 * time.Second)
	if err, _ := client2.Get("client2", "k"); err != 0 {
		log.Println(err)
	}

	if err, _ := client2.Put("client2", "k", "v3"); err != 0 {
		log.Println(err)
	}

	if err, _ := client2.Get("client2", "k"); err != 0 {
		log.Println(err)
	}

	if err, _ := client.Get(config.ClientID, "k"); err != 0 {
		log.Println(err)
	}
	//time.Sleep(time.Second * 3)
	if err, _ := client.Put(config.ClientID, "big", "v555"); err != 0 {
		log.Println(err)
	}

	if err, _ := client.Get(config.ClientID, "big"); err != 0 {
		log.Println(err)
	}

	if err, _ := client2.Put("client2", "k", "value3"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Put(config.ClientID, "k", "value3"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Put(config.ClientID, "k", "value4"); err != 0 {
		log.Println(err)
	}
	//time.Sleep(time.Second * 3)
	if err, _ := client.Put(config.ClientID, "k", "value5"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Put(config.ClientID, "k", "value6"); err != 0 {
		log.Println(err)
	}

	if err, _ := client.Get(config.ClientID, "k"); err != 0 {
		log.Println(err)
	}

	if err, _ := client2.Get("client2", "k"); err != 0 {
		log.Println(err)
	}

	if err, _ := client2.Get("client2", "big"); err != 0 {
		log.Println(err)
	}

	//for i := 0; i < 2; i++ {
	//	result := <-client.NotifyChannel
	//	log.Println(result)
	//}

	//if err, _ := client.Get("clientID1", "key2"); err != 0 {
	//	log.Println(err)
	//}

	for i := 0; i < 17; i++ {
		select {
		case mineResult := <-client.NotifyChannel:
			log.Println(mineResult)
			//case mineResult := <-client2.NotifyChannel:
			//	log.Println(mineResult)
			//case mineResult := <-client3.NotifyChannel:
			//	log.Println(mineResult)
			//case mineResult := <-client4.NotifyChannel:
			//	log.Println(mineResult)
		case mineResult2 := <-client2.NotifyChannel:
			log.Println(mineResult2)
		}
	}
}
