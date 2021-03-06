package main

import (
	"errors"
	"io"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type s3Bucket struct {
	config bucketConfig
	cfg    client.ConfigProvider
	conn   *s3.S3
	name   string
}

func (d *s3Bucket) Config() bucketConfig {
	return d.config
}

func newS3Bucket(cfg client.ConfigProvider, bc bucketConfig) (*s3Bucket, error) {
	conn := s3.New(cfg)
	bucket := &s3Bucket{
		conn:   conn,
		name:   bc.Name,
		cfg:    cfg,
		config: bc,
	}
	return bucket, nil
}

func (b *s3Bucket) Get(path string) (io.ReadCloser, error) {
	req := &s3.GetObjectInput{}
	req.Bucket = &b.name
	req.Key = &path
	res, err := b.conn.GetObject(req)
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

func (b *s3Bucket) Put(path string, data io.Reader) error {
	uploader := s3manager.NewUploader(b.cfg)
	if uploader == nil {
		return errors.New("Unable to create uploader")
	}

	req := &s3manager.UploadInput{}
	req.Bucket = &b.name
	req.Key = &path
	req.Body = data
	_, err := uploader.Upload(req)
	if err != nil {
		return err
	}

	return nil
}

func (b *s3Bucket) Head(path string) (bool, error) {
	req := &s3.HeadObjectInput{}
	req.Bucket = &b.name
	req.Key = &path

	_, err := b.conn.HeadObject(req)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *s3Bucket) Delete(path string) error {
	req := &s3.DeleteObjectInput{}
	req.Bucket = &b.name
	req.Key = &path

	_, err := b.conn.DeleteObject(req)
	if err != nil {
		return err
	}

	return nil
}

func (b *s3Bucket) List(prefix string) ([]string, error) {
	req := &s3.ListObjectsV2Input{}
	req.Bucket = &b.name
	if prefix != "" {
		req.Prefix = &prefix
	}

	dest := []string{}

	res, err := b.conn.ListObjectsV2(req)
	if err != nil {
		return dest, err
	}

	for _, obj := range res.Contents {
		dest = append(dest, *obj.Key)
	}

	return dest, nil
}
