package main

import (
	"bytes"
	"testing"
)

func TestDisk(t *testing.T) {
	d := newDiskBucket("./testing")

	buf := bytes.NewBuffer([]byte{})
	buf.WriteString("ABC123")

	if err := d.Put("test", buf); err != nil {
		t.Error(err)
	}

	if exists, err := d.Head("test"); err != nil {
		t.Error(err)
	} else if !exists {
		t.Fatal("Key does not exist")
	}

	if err := d.Delete("test"); err != nil {
		t.Error(err)
	}
}
