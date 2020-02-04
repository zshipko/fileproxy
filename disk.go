package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
)

type diskBucket struct {
	config bucketConfig
	root   string
}

func (d *diskBucket) Config() bucketConfig {
	return d.config
}

func (d *diskBucket) makePath(args ...string) string {
	return filepath.Join(d.root, filepath.Join(args...))
}

func (d *diskBucket) Get(key string) (io.ReadCloser, error) {
	f, err := os.Open(d.makePath(key))
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (d *diskBucket) Put(key string, value io.Reader) error {
	path := d.makePath(key)

	os.MkdirAll(filepath.Dir(path), 0766)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err = io.Copy(f, value); err != nil {
		return err
	}

	return nil
}

func (d *diskBucket) Head(key string) (bool, error) {
	_, err := os.Stat(d.makePath(key))

	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil
}

func (d *diskBucket) Delete(key string) error {
	path := d.makePath(key)
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}

	if stat.IsDir() {
		return os.RemoveAll(path)
	}

	return os.Remove(d.makePath(key))
}

func (d *diskBucket) List(prefix string) ([]string, error) {
	dest := []string{}

	err := filepath.Walk(d.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		path = strings.TrimPrefix(path, d.root)

		if prefix == "" && !strings.HasPrefix(path, prefix) {
			return nil
		}

		dest = append(dest, path)
		return nil
	})

	return dest, err
}

func (d *diskBucket) Upload() bool {
	return false
}

func (d *diskBucket) Cache() bool {
	return true
}

func newDiskBucket(root string) *diskBucket {
	os.MkdirAll(root, 0766)
	return &diskBucket{
		root: root,
	}
}
