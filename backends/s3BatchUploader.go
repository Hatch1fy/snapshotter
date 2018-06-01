package backends

import (
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func newBatchUploader(bucket string, ups ...UploadPair) (u S3BatchUploader) {
	u.bucket = bucket
	u.ups = ups
	return
}

// S3BatchUploader is an s3 batch uploader
type S3BatchUploader struct {
	bucket string

	ups   []UploadPair
	index int
}

// Next opens the next file and stops iteration if it fails to open
// a file.
func (u *S3BatchUploader) Next() bool {
	if u.index == len(u.ups) {
		return false
	}

	return true
}

// Err returns an error that was set during opening the file
func (u *S3BatchUploader) Err() error {
	return nil
}

// UploadObject returns a BatchUploadObject and sets the After field to
// close the file.
func (u *S3BatchUploader) UploadObject() (batch s3manager.BatchUploadObject) {
	up := u.ups[u.index]
	batch.Object = &s3manager.UploadInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(up.Key),
		Body:   up.Body,
	}

	u.index++
	return
}

// UploadPair is a key/body pair used for batch uploads
type UploadPair struct {
	Key  string
	Body io.Reader
}
