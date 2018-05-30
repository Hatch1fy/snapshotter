package backends

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Note: This test takes quite a while because it uploads 1001 items to test the iterator continuation
func TestS3Iterator(t *testing.T) {
	var err error
	creds := credentials.NewEnvCredentials()
	region := os.Getenv("AWS_REGION")
	bucket := fmt.Sprintf("%s.%d", "testing", time.Now().Unix())

	cfg := aws.Config{
		Region:      aws.String(region),
		Credentials: creds,
	}

	// The session the S3 Uploader will use
	sess := session.Must(session.NewSession(&cfg))

	// Create s3 service with the session and the default options
	svc := s3.New(sess, &cfg)

	var createinput s3.CreateBucketInput
	createinput.Bucket = aws.String(bucket)
	if _, err = svc.CreateBucket(&createinput); err != nil {
		t.Fatal(err)
	}

	var deleteInput s3.DeleteBucketInput
	deleteInput.Bucket = aws.String(bucket)
	defer svc.DeleteBucket(&deleteInput)

	// Create an uploader with the session and default options
	u := s3manager.NewUploader(sess)

	bs := []byte("hello world")
	ups := make([]UploadPair, 0, 1001)

	for i := 0; i < 1001; i++ {
		ups = append(ups, UploadPair{
			Key:  fmt.Sprintf("%05d", i),
			Body: bytes.NewReader(bs),
		})
	}

	bu := newBatchUploader(bucket, ups...)

	if err = u.UploadWithIterator(aws.BackgroundContext(), &bu); err != nil {
		t.Fatal(err)
	}

	s3i := newIterator(svc, bucket, "", "", -1)

	var cnt int
	for {
		if _, err = s3i.Next(); err != nil {
			break
		}

		cnt++
	}

	if err != io.EOF {
		t.Fatal(err)
	}

	if cnt != 1001 {
		t.Fatalf("invalid count, expected %d and received %d", 2000, cnt)
	}

	s3i = newIterator(svc, bucket, "", "", 7)
	cnt = 0

	for {
		if _, err = s3i.Next(); err != nil {
			break
		}

		cnt++
	}

	if err != io.EOF {
		t.Fatal(err)
	}

	if cnt != 7 {
		t.Fatalf("invalid count, expected %d and received %d", 7, cnt)
	}
}
