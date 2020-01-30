package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
)

var host = "127.0.0.1"
var port = 8081
var config = ""

type bucketConfig struct {
	Mode string `json:"mode"`
	Ref  string `json:"ref"`
}

func main() {
	s := newServer("", 8081)

	flag.StringVar(&s.host, "host", host, "Host to listen on")
	flag.IntVar(&s.port, "port", port, "Port to listen on")
	flag.StringVar(&config, "cfg", "", "Bucket config")
	flag.Parse()

	if config != "" {
		f, err := os.Open(config)
		if err != nil {
			log.Fatal("Unable to open bucket config: " + err.Error())
		}

		cfg := map[string]bucketConfig{}
		if err := json.NewDecoder(f).Decode(&cfg); err != nil {
			log.Fatal("Unable to decode bucket config: " + err.Error())
		}

		for k, v := range cfg {
			switch strings.ToLower(v.Mode) {
			case "local":
				fallthrough
			case "disk":
				s.buckets[k] = newDiskBucket(v.Ref)
			case "b2":
				if s.backblaze == nil {
					log.Fatal("Backblaze client not initialized, set B2_ACCOUNT_ID and B2_ACCOUNT_KEY environment variables")
				}

				s.buckets[k], err = newB2Bucket(s.backblaze, v.Ref)
				if err != nil {
					log.Fatal("Unable to create B2 bucket: " + err.Error())
				}
			case "s3":
				if s.aws == nil {
					log.Fatal("Amazon S3 client is not configured")
				}
				s.buckets[k], err = newS3Bucket(s.aws, v.Ref)
			default:
				log.Fatal("Unknown storage backend: " + v.Mode)
			}
		}
	}

	log.Println("fileproxy listening on:", s.addr())
	if err := http.ListenAndServe(s.addr(), &s); err != nil {
		log.Fatal(err)
	}
}
