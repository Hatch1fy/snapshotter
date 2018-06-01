package backends

import (
	"io"

	"github.com/Hatch1fy/errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	// Break is used as a break for an iterator
	Break = errors.Error("break")
)

// newIterator will return a new iterator
func newIterator(s3 *s3.S3, bucket, prefix, marker string, maxKeys int64) *S3Iterator {
	var s3i S3Iterator
	s3i.s3 = s3
	s3i.bucket = bucket
	s3i.marker = marker
	s3i.maxKeys = maxKeys
	return &s3i
}

// S3Iterator iterates through an s3 bucket
type S3Iterator struct {
	s3 *s3.S3

	bucket string
	prefix string
	marker string

	maxKeys int64
	curKeys int64

	index int

	output *s3.ListObjectsOutput
}

func (i *S3Iterator) newInput() *s3.ListObjectsInput {
	var input s3.ListObjectsInput
	input.Bucket = aws.String(i.bucket)

	if i.prefix != "" {
		input.Prefix = aws.String(i.prefix)
	}

	if i.marker != "" {
		input.Marker = aws.String(i.marker)
	}

	if i.maxKeys > 0 {
		input.MaxKeys = aws.Int64(i.maxKeys)
	}

	return &input
}

func (i *S3Iterator) getCurrentContent() *s3.Object {
	return i.output.Contents[i.index]
}

func (i *S3Iterator) getTailContent() *s3.Object {
	// Tail index is the last index in the current set of contents
	tail := len(i.output.Contents) - 1
	// Return our tail reference
	return i.output.Contents[tail]
}

func (i *S3Iterator) clearOutput() {
	// Ensure output isn't already nil
	if i.output == nil {
		// Output is nil, return
		return
	}

	// Ensure we've iterated through all the contents
	if i.index < len(i.output.Contents) {
		// We haven't iterated through all the contents, return
		return
	}

	// Get tail content
	tail := i.getTailContent()
	// Set marker as the last key
	i.marker = *tail.Key
	// Reset index to zero
	i.index = 0
	// Set output to nil
	i.output = nil
}

func (i *S3Iterator) setOutput() (err error) {
	// Ensure output is nil
	if i.output != nil {
		// Output is not nil, return
		return
	}

	// Create input
	input := i.newInput()

	// Set output as a new Objects list
	i.output, err = i.s3.ListObjects(input)
	return
}

// Len will return the length of the iterator
func (i *S3Iterator) Len() (n int) {
	if i.output == nil {
		return
	}

	return len(i.output.Contents)
}

// Cap will return the capacity of the iterator
func (i *S3Iterator) Cap() (n int) {
	if i.output == nil {
		return
	}

	return int(i.maxKeys)
}

// Next will iterate through the next item
func (i *S3Iterator) Next() (key string, err error) {
	// Ensure we haven't reached out maxKeys value
	if i.curKeys == i.maxKeys {
		// We have reached our max, return end of file
		err = io.EOF
		return
	}

	// Clear the current output (if needed)
	i.clearOutput()

	// Set the current output (if needed)
	if err = i.setOutput(); err != nil {
		return
	}

	if len(i.output.Contents) == 0 {
		err = io.EOF
		return
	}

	// Get the current content
	current := i.getCurrentContent()
	// Set our return key as the value of the current key
	key = *current.Key
	// Increment index
	i.index++
	// Increment current keys
	i.curKeys++
	return
}
