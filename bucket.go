package main

import "io"

type bucket interface {
	Get(key string) (io.ReadCloser, error)
	Put(key string, value io.Reader) error
	Head(key string) (bool, error)
	Delete(key string) error
	List(prefix string) ([]string, error)
	Config() bucketConfig
}
