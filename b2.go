package main

import (
	"context"
	"errors"
	"io"

	"github.com/kurin/blazer/b2"
)

type b2Bucket struct {
	config bucketConfig
	client *b2.Client
	bucket *b2.Bucket
	name   string
}

func (d *b2Bucket) Config() bucketConfig {
	return d.config
}

func newB2Bucket(bc bucketConfig) (*b2Bucket, error) {
	ctx := context.Background()

	c, err := b2.NewClient(ctx, bc.ApiID, bc.ApiKey)
	if err != nil {
		return nil, err
	}

	b, err := c.NewBucket(ctx, bc.Name, nil)
	if err != nil {
		return nil, err
	}

	bucket := &b2Bucket{
		name:   bc.Name,
		client: c,
		bucket: b,
		config: bc,
	}

	return bucket, nil
}

func (b *b2Bucket) Get(key string) (io.ReadCloser, error) {
	obj := b.bucket.Object(key)
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
	obj := b.bucket.Object(key)
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
	iter := b.bucket.List(context.Background())
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
	obj := b.bucket.Object(key)
	if obj == nil {
		return errors.New("Unable to create Object")
	}

	return obj.Delete(context.Background())
}

func (b *b2Bucket) List(prefix string) ([]string, error) {
	opts := b2.ListPrefix(prefix)
	obj := b.bucket.List(context.Background(), opts)

	dest := []string{}

	if err := obj.Err(); err != nil {
		return dest, err
	}

	for obj.Next() {
		dest = append(dest, obj.Object().Name())
	}

	return dest, nil
}
