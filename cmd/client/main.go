package main

import (
	distkvs "example.org/cpsc416/a5"
	"example.org/cpsc416/a5/kvslib"
	"log"
	"time"
)

// ./gradera6-smoketest -c client1 client2 client3 client5 client6 client7 client8 client4 -s storage1 storage2 -r client1:put:4:k client2:put:4:k client1:get:4:k client2:get:4:k client3:put:4:k client4:put:4:k client3:get:4:k client4:get:4:k client5:put:4:k client6:put:4:k client7:get:4:k client8:get:4:k client5:put:4:k client6:put:4:k client7:get:4:k client8:get:4:k client1:put:4:k2 client2:put:4:k2 client1:get:4:k2 client2:get:4:k2 client3:put:4:k2 client4:put:4:k2 client3:get:4:k2 client4:get:4:k2 client5:put:4:k2 client6:put:4:k2 client7:get:4:k2 client8:get:4:k2 client5:put:4:k2 client6:put:4:k2 client7:get:4:k2 client8:get:4:k2 -t trace_output.log > grading.txt
func main() {
	var config distkvs.ClientConfig
	err := distkvs.ReadJSONConfig("config/client_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	//flag.StringVar(&config.ClientID, "id", config.ClientID, "Client ID, e.g. client1")
	//flag.Parse()
	log.Println(config)

	var config2 distkvs.ClientConfig
	err = distkvs.ReadJSONConfig("config/client2_config.json", &config2)
	if err != nil {
		log.Fatal(err)
	}
	//flag.StringVar(&config2.ClientID, "id2", config2.ClientID, "Client ID, e.g. client1")
	//flag.Parse()
	log.Println(config2)

	var config3 distkvs.ClientConfig
	err = distkvs.ReadJSONConfig("config/client3_config.json", &config3)
	if err != nil {
		log.Fatal(err)
	}
	//flag.StringVar(&config3.ClientID, "id3", config3.ClientID, "Client ID, e.g. client1")
	//flag.Parse()
	log.Println(config3)

	var config4 distkvs.ClientConfig
	err = distkvs.ReadJSONConfig("config/client4_config.json", &config4)
	if err != nil {
		log.Fatal(err)
	}
	//flag.StringVar(&config4.ClientID, "id4", config4.ClientID, "Client ID, e.g. client1")
	//flag.Parse()
	log.Println(config4)

	var config5 distkvs.ClientConfig
	err = distkvs.ReadJSONConfig("config/client5_config.json", &config5)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(config5)

	var config6 distkvs.ClientConfig
	err = distkvs.ReadJSONConfig("config/client6_config.json", &config6)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(config6)

	var config7 distkvs.ClientConfig
	err = distkvs.ReadJSONConfig("config/client7_config.json", &config7)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(config7)

	var config8 distkvs.ClientConfig
	err = distkvs.ReadJSONConfig("config/client8_config.json", &config8)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(config8)

	client := distkvs.NewClient(config, kvslib.NewKVS())
	if err := client.Initialize(); err != nil {
		log.Fatal(err)
	}
	client2 := distkvs.NewClient(config2, kvslib.NewKVS())
	if err := client2.Initialize(); err != nil {
		log.Fatal(err)
	}
	client3 := distkvs.NewClient(config3, kvslib.NewKVS())
	if err := client3.Initialize(); err != nil {
		log.Fatal(err)
	}
	client4 := distkvs.NewClient(config4, kvslib.NewKVS())
	if err := client4.Initialize(); err != nil {
		log.Fatal(err)
	}

	//client5 := distkvs.NewClient(config5, kvslib.NewKVS())
	//if err := client5.Initialize(); err != nil {
	//	log.Fatal(err)
	//}
	//client6 := distkvs.NewClient(config6, kvslib.NewKVS())
	//if err := client6.Initialize(); err != nil {
	//	log.Fatal(err)
	//}
	//client7 := distkvs.NewClient(config7, kvslib.NewKVS())
	//if err := client7.Initialize(); err != nil {
	//	log.Fatal(err)
	//}
	//client8 := distkvs.NewClient(config8, kvslib.NewKVS())
	//if err := client8.Initialize(); err != nil {
	//	log.Fatal(err)
	//}
	defer client.Close()
	defer client2.Close()
	defer client3.Close()
	defer client4.Close()
	//defer client5.Close()
	//defer client6.Close()
	//defer client7.Close()
	//defer client8.Close()
	if err, _ := client.Put(config.ClientID, "k", "v1"); err != 0 {
		log.Println(err)
	}
	//time.Sleep(10*time.Second)
	if err, _ := client2.Put("client2", "k", "v2"); err != 0 {
		log.Println(err)
	}

	if err, _ := client3.Put("client3", "k", "v3"); err != 0 {
		log.Println(err)
	}
	time.Sleep(10*time.Second)
	if err, _ := client4.Put("client4", "k", "v4"); err != 0 {
		log.Println(err)
	}

	if err, _ := client.Get(config.ClientID, "k"); err != 0 {
		log.Println(err)
	}
	if err, _ := client2.Get("client2", "k"); err != 0 {
		log.Println(err)
	}
	if err, _ := client3.Get("client3", "k"); err != 0 {
		log.Println(err)
	}
	//time.Sleep(3*time.Second)
	if err, _ := client4.Get("client4", "k"); err != 0 {
		log.Println(err)
	}


	//if err, _ := client5.Put("client5", "k", "v234"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client6.Put("client6", "k", "345"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client7.Put("client7", "k", "756"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client8.Put("client8", "k", "97"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client5.Get("client5", "k"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client6.Get("client6", "k"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client7.Get("client7", "k"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client8.Get("client8", "k"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client.Put(config.ClientID, "k2", "v1"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client2.Put("client2", "k2", "v2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client3.Put("client3", "k2", "v3"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client4.Put("client4", "k2", "v4"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client.Get(config.ClientID, "k2"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client2.Get("client2", "k2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client3.Get("client3", "k2"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client4.Get("client4", "k2"); err != 0 {
	//	log.Println(err)
	//}
	//
	//
	//if err, _ := client5.Put("client5", "k2", "v234"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client6.Put("client6", "k2", "345"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client7.Put("client7", "k2", "756"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client8.Put("client8", "k2", "97"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client5.Get("client5", "k2"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client6.Get("client6", "k2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client7.Get("client7", "k2"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client8.Get("client8", "k2"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client.Put(config.ClientID, "k", "v1"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client2.Put("client2", "k", "v2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client3.Put("client3", "k", "v3"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client4.Put("client4", "k", "v4"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client.Get(config.ClientID, "k"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client2.Get("client2", "k"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client3.Get("client3", "k"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client4.Get("client4", "k"); err != 0 {
	//	log.Println(err)
	//}
	//
	//
	//if err, _ := client5.Put("client5", "k", "v234"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client6.Put("client6", "k", "345"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client7.Put("client7", "k", "756"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client8.Put("client8", "k", "97"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client5.Get("client5", "k"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client6.Get("client6", "k"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client7.Get("client7", "k"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client8.Get("client8", "k"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client.Put(config.ClientID, "k2", "v1"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client2.Put("client2", "k2", "v2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client3.Put("client3", "k2", "v3"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client4.Put("client4", "k2", "v4"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client.Get(config.ClientID, "k2"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client2.Get("client2", "k2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client3.Get("client3", "k2"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client4.Get("client4", "k2"); err != 0 {
	//	log.Println(err)
	//}
	//
	//
	//if err, _ := client5.Put("client5", "k2", "v234"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client6.Put("client6", "k2", "345"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client7.Put("client7", "k2", "756"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client8.Put("client8", "k2", "97"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client5.Get("client5", "k2"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client6.Get("client6", "k2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client7.Get("client7", "k2"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client8.Get("client8", "k2"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client.Put(config.ClientID, "k", "v1"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client2.Put("client2", "k", "v2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client3.Put("client3", "k", "v3"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client4.Put("client4", "k", "v4"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client.Get(config.ClientID, "k"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client2.Get("client2", "k"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client3.Get("client3", "k"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client4.Get("client4", "k"); err != 0 {
	//	log.Println(err)
	//}
	//
	//
	//if err, _ := client5.Put("client5", "k", "v234"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client6.Put("client6", "k", "345"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client7.Put("client7", "k", "756"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client8.Put("client8", "k", "97"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client5.Get("client5", "k"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client6.Get("client6", "k"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client7.Get("client7", "k"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client8.Get("client8", "k"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client.Put(config.ClientID, "k2", "v1"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client2.Put("client2", "k2", "v2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client3.Put("client3", "k2", "v3"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client4.Put("client4", "k2", "v4"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client.Get(config.ClientID, "k2"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client2.Get("client2", "k2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client3.Get("client3", "k2"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client4.Get("client4", "k2"); err != 0 {
	//	log.Println(err)
	//}
	//
	//
	//if err, _ := client5.Put("client5", "k2", "v234"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client6.Put("client6", "k2", "345"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client7.Put("client7", "k2", "756"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client8.Put("client8", "k2", "97"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client5.Get("client5", "k2"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client6.Get("client6", "k2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client7.Get("client7", "k2"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client8.Get("client8", "k2"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client.Put(config.ClientID, "k", "v1"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client2.Put("client2", "k", "v2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client3.Put("client3", "k", "v3"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client4.Put("client4", "k", "v4"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client.Get(config.ClientID, "k"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client2.Get("client2", "k"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client3.Get("client3", "k"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client4.Get("client4", "k"); err != 0 {
	//	log.Println(err)
	//}
	//
	//
	//if err, _ := client5.Put("client5", "k", "v234"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client6.Put("client6", "k", "345"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client7.Put("client7", "k", "756"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client8.Put("client8", "k", "97"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client5.Get("client5", "k"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client6.Get("client6", "k"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client7.Get("client7", "k"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client8.Get("client8", "k"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client.Put(config.ClientID, "k2", "v1"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client2.Put("client2", "k2", "v2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client3.Put("client3", "k2", "v3"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client4.Put("client4", "k2", "v4"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client.Get(config.ClientID, "k2"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client2.Get("client2", "k2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client3.Get("client3", "k2"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client4.Get("client4", "k2"); err != 0 {
	//	log.Println(err)
	//}
	//
	//
	//if err, _ := client5.Put("client5", "k2", "v234"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client6.Put("client6", "k2", "345"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client7.Put("client7", "k2", "756"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client8.Put("client8", "k2", "97"); err != 0 {
	//	log.Println(err)
	//}
	//
	//if err, _ := client5.Get("client5", "k2"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client6.Get("client6", "k2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client7.Get("client7", "k2"); err != 0 {
	//	log.Println(err)
	//}
	////time.Sleep(3*time.Second)
	//if err, _ := client8.Get("client8", "k2"); err != 0 {
	//	log.Println(err)
	//}




	//for i := 0; i < 2; i++ {
	//	result := <-client.NotifyChannel
	//	log.Println(result)
	//}

	//if err, _ := client.Get("clientID1", "key2"); err != 0 {
	//	log.Println(err)
	//}

	for i := 0; i < 8; i++ {
		select {
		case mineResult := <-client.NotifyChannel:
			log.Println(mineResult)
		case mineResult2 := <-client2.NotifyChannel:
			log.Println(mineResult2)
		case mineResult3 := <-client3.NotifyChannel:
			log.Println(mineResult3)
		case mineResult4 := <-client4.NotifyChannel:
			log.Println(mineResult4)
		//case mineResult := <-client5.NotifyChannel:
		//	log.Println(mineResult)
		//case mineResult2 := <-client6.NotifyChannel:
		//	log.Println(mineResult2)
		//case mineResult3 := <-client7.NotifyChannel:
		//	log.Println(mineResult3)
		//case mineResult4 := <-client8.NotifyChannel:
		//	log.Println(mineResult4)
		}
	}
}
