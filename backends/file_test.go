package backends

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestFile(t *testing.T) {
	var (
		fb  *File
		err error
	)

	if err = os.MkdirAll("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("test_data")

	fb = NewFile("test_data")

	// Attempt to write to test key
	if err = fb.WriteTo("test_log_1.log", func(w io.Writer) (err error) {
		_, err = w.Write([]byte("hello world\n"))
		return
	}); err != nil {
		t.Fatal(err)
	}

	// Attempt to write to test key
	if err = fb.WriteTo("test_log_2.log", func(w io.Writer) (err error) {
		_, err = w.Write([]byte("hello world\n"))
		return
	}); err != nil {
		t.Fatal(err)
	}

	// Attempt to read from test key
	if err = fb.ReadFrom("test_log_1.log", func(r io.Reader) (err error) {
		// Create buffer to writer to
		buf := bytes.NewBuffer(nil)
		// Copy reader bytes to buffer
		if _, err = io.Copy(buf, r); err != nil {
			return
		}

		// Ensure buffer value is our expected value
		if buf.String() != "hello world\n" {
			return fmt.Errorf("invalid value, expected \"%s\" and received \"%s\"", "hello world", buf.String())
		}

		return
	}); err != nil {
		t.Fatal(err)
	}

	var nextKey string
	if nextKey, err = fb.Next("test", ""); err != nil {
		t.Fatal(err)
	} else if nextKey != "test_log_1.log" {
		t.Fatalf("invalid key value, expected \"%s\" and received \"%s\"", "test_log_1.log", nextKey)
	}

	if nextKey, err = fb.Next("test", nextKey); err != nil {
		t.Fatal(err)
	} else if nextKey != "test_log_2.log" {
		t.Fatalf("invalid key value, expected \"%s\" and received \"%s\"", "test_log_2.log", nextKey)
	}

	if nextKey, err = fb.Next("test", nextKey); err != io.EOF {
		t.Fatalf("io.EOF expected, received: %v", err)
	}
}
