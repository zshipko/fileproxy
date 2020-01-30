package main

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/kurin/blazer/b2"
)

type b2Bucket struct {
	client *b2.Bucket
	name   string
}

var b2id = ""
var b2key = ""

func init() {
	b2id = os.Getenv("B2_ACCOUNT_ID")
	b2key = os.Getenv("B2_ACCOUNT_KEY")
}

func newB2Bucket(client *b2.Client, name string) (*b2Bucket, error) {
	b, err := client.NewBucket(context.Background(), name, nil)
	if err != nil {
		return nil, err
	}

	bucket := &b2Bucket{
		name:   name,
		client: b,
	}

	return bucket, nil
}

func (b *b2Bucket) Get(key string) (io.ReadCloser, error) {
	obj := b.client.Object(key)
	if obj == nil {
		return nil, errors.New("Unable to create Object")
	}
	r := obj.NewReader(context.Background())
	if r == nil {
		return nil, errors.New("Unable to create Reader")
	}
	return r, nil
}

func (b *b2Bucket) Put(key string, data io.Reader) error {
	obj := b.client.Object(key)
	if obj == nil {
		return errors.New("Unable to create Object")
	}

	w := obj.NewWriter(context.Background())
	if w == nil {
		return errors.New("Unable to create writer")
	}

	defer w.Close()

	_, err := io.Copy(w, data)
	return err
}

func (b *b2Bucket) Head(key string) (bool, error) {
	iter := b.client.List(context.Background())
	if iter == nil {
		return false, errors.New("Unable to list objects")
	}

	for iter.Next() {
		obj := iter.Object()
		if obj.Name() == key {
			return true, nil
		}
	}

	return false, nil
}

func (b *b2Bucket) Delete(key string) error {
	obj := b.client.Object(key)
	if obj == nil {
		return errors.New("Unable to create Object")
	}

	return obj.Delete(context.Background())
}
