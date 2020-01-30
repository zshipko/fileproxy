package main

import (
	"errors"
	"io"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type s3Bucket struct {
	cfg  client.ConfigProvider
	conn *s3.S3
	name string
}

func newS3Bucket(cfg client.ConfigProvider, name string) (*s3Bucket, error) {
	conn := s3.New(cfg)
	bucket := &s3Bucket{
		conn: conn,
		name: name,
		cfg:  cfg,
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
