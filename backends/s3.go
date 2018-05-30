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
	var s3b S3Backend
	// The session the S3 Uploader will use
	sess := session.Must(session.NewSession(&cfg))

	// Create s3 service with the session and the default options
	s3b.s = s3.New(sess, &cfg)
	// Create an uploader with the session and default options
	s3b.u = s3manager.NewUploader(sess)
	// Create a downloader with the session and default options
	s3b.d = s3manager.NewDownloader(sess)

	// Set s3 bucket
	s3b.bucket = bucket
	// Assign S3's pointer
	sp = &s3b
	return
}

// S3Backend manages the Amazon S3 backend
type S3Backend struct {
	s *s3.S3
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

// newIterator will return a new iterator
func (s *S3Backend) newIterator(prefix, marker string, maxKeys int64) (output *s3.ListObjectsOutput, err error) {
	input := &s3.ListObjectsInput{
		Bucket:  aws.String(s.bucket),
		Prefix:  aws.String(prefix),
		Marker:  aws.String(marker),
		MaxKeys: aws.Int64(maxKeys),
	}

	return s.s.ListObjects(input)
}

// ForEach will iterate through all the keys
func (s *S3Backend) ForEach(prefix, marker string, maxKeys int64, fn func(key string) (err error)) (err error) {
	iter := newIterator(s.s, s.bucket, prefix, marker, maxKeys)

	// Iterate until error
	for {
		var key string
		// Get next key
		if key, err = iter.Next(); err != nil {
			// Error encountered, break
			break
		}

		if err = fn(key); err != nil {
			break
		}
	}

	if err == io.EOF || err == Break {
		err = nil
	}

	return
}

// List will list the backend keys
func (s *S3Backend) List(prefix, marker string, maxKeys int64) (keys []string, err error) {
	iter := newIterator(s.s, s.bucket, prefix, marker, maxKeys)

	// Iterate until error
	for {
		var key string
		// Get next key
		if key, err = iter.Next(); err != nil {
			// Error encountered, break
			break
		}

		if len(keys) == 0 {
			// We haven't created keys yet, pre-allocate keys as iterator length
			keys = make([]string, 0, iter.Len())
		}

		// Append key to keys slice
		keys = append(keys, key)
	}

	if err == io.EOF {
		err = nil
	}

	return
}
