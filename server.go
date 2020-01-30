package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/kurin/blazer/b2"
)

type server struct {
	host    string
	port    int
	buckets map[string]bucket
	router  *http.ServeMux

	backblaze *b2.Client
	aws       client.ConfigProvider
}

func (s server) handler(w http.ResponseWriter, r *http.Request) {
	path := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

	log.Println("PATH:", path)

	if len(path) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	bucket := path[0]
	newPath := filepath.Join(path[1:]...)

	if b, ok := s.buckets[bucket]; ok {
		switch strings.ToLower(r.Method) {
		case "get":
			x, err := b.Get(newPath)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer x.Close()

			if _, err = io.Copy(w, x); err != nil {
				log.Println("ERROR:", err)
			}
		case "post":
			fallthrough
		case "put":
			defer r.Body.Close()
			if err := b.Put(newPath, r.Body); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.WriteHeader(200)
		case "delete":
			if err := b.Delete(newPath); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.WriteHeader(200)
		case "head":
			exists, err := b.Head(newPath)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if !exists {
				http.Error(w, "", http.StatusNotFound)
			} else {
				w.WriteHeader(200)
			}
		default:
			http.Error(w, "Invalid method: "+r.Method, http.StatusMethodNotAllowed)
		}
	} else {
		http.Error(w, "Not found", http.StatusNotFound)
	}
}

func newServer(host string, port int) server {
	var err error

	s := server{
		host: host,
		port: port,
		buckets: map[string]bucket{
			"tmp": newDiskBucket("/tmp/fileproxy"),
		},
		router: http.NewServeMux(),
	}

	if b2id != "" && b2key != "" {
		s.backblaze, err = b2.NewClient(context.Background(), b2id, b2key)
		if err != nil {
			log.Fatal("Unable to connect to Backblaze B2: " + err.Error())
		}
	}

	s.aws, _ = session.NewSession()

	s.router.HandleFunc("/", s.handler)

	return s
}

func (s server) addr() string {
	return fmt.Sprint(s.host, ":", s.port)
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}
