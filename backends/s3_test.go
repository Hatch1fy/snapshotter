package backends

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

func TestS3(t *testing.T) {
	var (
		s3  *S3
		err error
	)

	creds := credentials.NewEnvCredentials()
	region := os.Getenv("AWS_REGION")
	bucket := os.Getenv("AWS_BUCKET")

	cfg := aws.Config{
		Region:      aws.String(region),
		Credentials: creds,
	}

	if s3, err = NewS3(cfg, bucket); err != nil {
		t.Fatal(err)
	}

	// Attempt to write to test key
	if err = s3.WriteTo("test", func(w io.Writer) (err error) {
		_, err = w.Write([]byte("hello world"))
		return
	}); err != nil {
		t.Fatal(err)
	}

	// Attempt to read from test key
	if err = s3.ReadFrom("test", func(r io.Reader) (err error) {
		// Create buffer to writer to
		buf := bytes.NewBuffer(nil)
		// Copy reader bytes to buffer
		if _, err = io.Copy(buf, r); err != nil {
			return
		}

		// Ensure buffer value is our expected value
		if buf.String() != "hello world" {
			return fmt.Errorf("invalid value, expected \"%s\" and received \"%s\"", "hello world", buf.String())
		}

		return
	}); err != nil {
		t.Fatal(err)
	}
}
