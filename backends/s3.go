package backends

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// NewS3Backend will return a new instance of S3
func NewS3Backend(cfg aws.Config, bucket string) (sp *S3Backend, err error) {
	var s3 S3Backend
	// The session the S3 Uploader will use
	sess := session.Must(session.NewSession(&cfg))

	// Create an uploader with the session and default options
	s3.u = s3manager.NewUploader(sess)
	// Create a downloader with the session and default options
	s3.d = s3manager.NewDownloader(sess)
	// Set s3 bucket
	s3.bucket = bucket
	// Assign S3's pointer
	sp = &s3
	return
}

// S3Backend manages the Amazon S3 backend
type S3Backend struct {
	u *s3manager.Uploader
	d *s3manager.Downloader

	bucket string
}

func (s *S3Backend) newObjectInput(key string) (objInput s3.GetObjectInput) {
	objInput.Bucket = aws.String(s.bucket)
	objInput.Key = aws.String(key)
	return
}

func (s *S3Backend) newUploadInput(key string, r io.Reader) (input s3manager.UploadInput) {
	input.Bucket = aws.String(s.bucket)
	input.Key = aws.String(key)
	input.Body = r
	return
}

func (s *S3Backend) upload(key string, r io.Reader) (err error) {
	// Create new upload input
	input := s.newUploadInput(key, r)
	// Upload writer to amazon
	_, err = s.u.Upload(&input)
	return
}

// WriteTo will write to a writer
func (s *S3Backend) WriteTo(key string, fn func(io.Writer) error) (err error) {
	var tmp *os.File
	if tmp, err = ioutil.TempFile("", "s3_backend"); err != nil {
		return
	}
	defer os.Remove(tmp.Name())

	// Write to temporary file
	if err = fn(tmp); err != nil {
		return
	}

	// Sync temproary file to ensure the bytes have made it to disk
	if err = tmp.Sync(); err != nil {
		return
	}

	// Seek to beginning of file
	if _, err = tmp.Seek(0, 0); err != nil {
		return
	}

	// Upload file to amazon
	return s.upload(key, tmp)
}

// ReadFrom will pass a reader to the provided function
func (s *S3Backend) ReadFrom(key string, fn func(io.Reader) error) (err error) {
	var tmp *os.File
	// Create temporary file to write to
	if tmp, err = ioutil.TempFile("", "s3_backend"); err != nil {
		// Error encountered while creating temporary file, return
		return
	}
	// Defer the removal of the temporary file
	defer os.Remove(tmp.Name())
	// Defer the close of the temporary file
	defer tmp.Close()
	// Create new object input
	objInput := s.newObjectInput(key)
	// Download the object input request to the temporary file
	if _, err = s.d.Download(tmp, &objInput); err != nil {
		// Error encountered while downloading, return
		return
	}
	// Seek the file to the beginning
	if _, err = tmp.Seek(0, 0); err != nil {
		return
	}
	// Call function and pass temporary file as reader
	return fn(tmp)
}
