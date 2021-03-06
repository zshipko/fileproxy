package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type server struct {
	host    string
	port    int
	buckets []bucket
	router  *http.ServeMux
}

func (s *server) handler(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")

	if len(path) < 1 {
		if strings.ToLower(r.Method) != "get" {
			http.Error(w, "Invalid path", http.StatusBadRequest)
			return
		}

		prefix := r.URL.Query().Get("prefix")
		nameSet := map[string]bool{}
		for i, b := range s.buckets {
			buckets, err := b.List(prefix)
			if err != nil {
				continue
			} else if err != nil && i == len(s.buckets)-1 && len(nameSet) == 0 {
				http.Error(w, "Invalid path", http.StatusBadRequest)
				return
			}

			for _, x := range buckets {
				nameSet[x] = true
			}
		}

		dest := []string{}
		for x := range nameSet {
			dest = append(dest, x)
		}

		json.NewEncoder(w).Encode(dest)
		return
	}

	for i, b := range s.buckets {
		switch strings.ToLower(r.Method) {
		case "get":
			x, err := b.Get(path)
			if err != nil {
				if i == len(s.buckets)-1 {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				continue
			}
			defer x.Close()

			if _, err = io.Copy(w, x); err != nil {
				if i == len(s.buckets)-1 {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				continue
			}

			return
		case "post":
			fallthrough
		case "put":
			defer r.Body.Close()

			if b.Config().Upload {
				if err := b.Put(path, r.Body); err != nil {
					if i == len(s.buckets)-1 {
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}
				}

				return
			}
		case "delete":
			if err := b.Delete(path); err != nil {
				if i == len(s.buckets)-1 {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				return
			}
		case "head":
			exists, err := b.Head(path)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if !exists && i == len(s.buckets)-1 {
				http.Error(w, "", http.StatusNotFound)
				return
			}
		default:
			http.Error(w, "Invalid method: "+r.Method, http.StatusMethodNotAllowed)
		}
	}
}

func newServer(host string, port int) *server {
	s := &server{
		host:    host,
		port:    port,
		buckets: []bucket{},
		router:  http.NewServeMux(),
	}

	s.router.HandleFunc("/", s.handler)

	return s
}

func (s *server) addr() string {
	return fmt.Sprint(s.host, ":", s.port)
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}
