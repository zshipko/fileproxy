package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
)

var host = "127.0.0.1"
var port = 8081
var config = filepath.Join(os.Getenv("HOME"), ".fileproxy.json")

type bucketConfig struct {
	Backend string `json:"backend"`
	Name    string `json:"name"`
	ApiKey  string `json:"api_key,omitempty"`
	ApiID   string `json:"api_id,omitempty"`
	Upload  bool   `json:"upload,omitempty"`
	Cache   bool   `json:"cache,omitempty"`
}

func main() {
	s := newServer("", 0)

	flag.StringVar(&s.host, "host", host, "Host to listen on")
	flag.IntVar(&s.port, "port", port, "Port to listen on")
	flag.StringVar(&config, "buckets", "", "Bucket list")
	flag.Parse()

	if config != "" {
		f, err := os.Open(config)
		if err != nil {
			log.Fatal("Unable to open bucket config: " + err.Error())
		}

		cfg := []bucketConfig{}
		if err := json.NewDecoder(f).Decode(&cfg); err != nil {
			log.Fatal("Unable to decode bucket config: " + err.Error())
		}

		for _, v := range cfg {
			switch strings.ToLower(v.Backend) {
			case "":
				fallthrough
			case "local":
				fallthrough
			case "disk":
				s.buckets = append(s.buckets, newDiskBucket(v.Name))
			case "backblaze":
				fallthrough
			case "b2":
				bucket, err := newB2Bucket(v.ApiID, v.ApiKey, v.Name)
				if err != nil {
					log.Fatal("Unable to create B2 bucket: " + err.Error())
				}
				s.buckets = append(s.buckets, bucket)
			case "aws":
				fallthrough
			case "s3":
				aws := session.Must(session.NewSession())
				bucket, err := newS3Bucket(aws, v.Name)
				if err != nil {
					log.Fatal("Unable to create S3 bucket: " + err.Error())
				}
				s.buckets = append(s.buckets, bucket)
			default:
				log.Fatal("Unknown storage backend: " + v.Backend)
			}
		}
	}

	log.Println("fileproxy listening on:", s.addr())
	if err := http.ListenAndServe(s.addr(), s); err != nil {
		log.Fatal(err)
	}
}
