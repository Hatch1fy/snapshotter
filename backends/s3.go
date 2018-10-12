package backends

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var defaultS3UploadOpts S3UploadOpts

// NewS3 will return a new instance of S3
func NewS3(cfg aws.Config, bucket string) (sp *S3, err error) {
	var s3b S3
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

// S3 manages the Amazon S3 backend
type S3 struct {
	s *s3.S3
	u *s3manager.Uploader
	d *s3manager.Downloader

	bucket string
}

func (s *S3) newObjectInput(key string) (objInput s3.GetObjectInput) {
	objInput.Bucket = aws.String(s.bucket)
	objInput.Key = aws.String(key)
	return
}

func (s *S3) newUploadInput(key string, r io.Reader, opts S3UploadOpts) (input s3manager.UploadInput) {
	input.Bucket = aws.String(s.bucket)
	input.Key = aws.String(key)
	input.Body = r

	// Set options
	input.CacheControl = opts.GetCacheControl()
	input.ContentDisposition = opts.GetContentDisposition()
	input.ContentEncoding = opts.GetContentEncoding()
	input.ContentLanguage = opts.GetContentLanguage()
	input.ContentMD5 = opts.GetContentMD5()
	input.ContentType = opts.GetContentType()
	return
}

func (s *S3) newDeleteInput(key string) (input s3.DeleteObjectInput) {
	input.Bucket = aws.String(s.bucket)
	input.Key = aws.String(key)
	return
}

// newIterator will return a new iterator
func (s *S3) newIterator(prefix, marker string, maxKeys int64) (output *s3.ListObjectsOutput, err error) {
	input := &s3.ListObjectsInput{
		Bucket:  aws.String(s.bucket),
		Prefix:  aws.String(prefix),
		Marker:  aws.String(marker),
		MaxKeys: aws.Int64(maxKeys),
	}

	return s.s.ListObjects(input)
}

func (s *S3) upload(key string, r io.Reader, opts S3UploadOpts) (location string, err error) {
	// Create new upload input
	input := s.newUploadInput(key, r, opts)

	var out *s3manager.UploadOutput
	// Upload writer to amazon
	if out, err = s.u.Upload(&input); err != nil {
		return
	}

	location = out.Location
	return
}

func (s *S3) delete(key string) (err error) {
	// Create new delete input
	input := s.newDeleteInput(key)
	// Delete key from amazon
	_, err = s.s.DeleteObject(&input)
	return
}

// WriteTo will write to a writer
func (s *S3) WriteTo(key string, fn func(io.Writer) error) (err error) {
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
	_, err = s.upload(key, tmp, defaultS3UploadOpts)
	return
}

// Upload will upload a reader to s3
func (s *S3) Upload(key string, r io.Reader, opts S3UploadOpts) (location string, err error) {
	// Upload file to amazon
	return s.upload(key, r, opts)
}

// ReadFrom will pass a reader to the provided function
func (s *S3) ReadFrom(key string, fn func(io.Reader) error) (err error) {
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

// Delete will delete a file from the s3 backend
func (s *S3) Delete(key string) (err error) {
	return s.delete(key)
}

// ForEach will iterate through all the keys
func (s *S3) ForEach(prefix, marker string, maxKeys int64, fn func(key string) (err error)) (err error) {
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

// Next will return the next key
func (s *S3) Next(prefix, current string) (nextKey string, err error) {
	// Create new iterator
	iter := newIterator(s.s, s.bucket, prefix, current, 1)
	// Get next key
	return iter.Next()
}

// List will list the backend keys
func (s *S3) List(prefix, marker string, maxKeys int64) (keys []string, err error) {
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

// NewS3Config will return a new parsed S3 configuration from a toml source
func NewS3Config(src string) (s S3Config, err error) {
	_, err = toml.DecodeFile(src, &s)
	return
}

// S3Config represents an S3 configuration
type S3Config struct {
	AccessKey string `toml:"accessKey"`
	SecretKey string `toml:"secretKey"`
	Region    string `toml:"region"`
	Bucket    string `toml:"buket"`
}

// Config returns the aws configuration
func (s *S3Config) Config() (cfg aws.Config) {
	cfg.Credentials = credentials.NewStaticCredentials(s.AccessKey, s.SecretKey, "")
	cfg.Region = aws.String(s.Region)
	return
}
