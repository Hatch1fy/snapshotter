package backends

import (
	"io"
	"os"
	"path"
)

// NewFilebackend will return a new instance of Filebackend
func NewFilebackend(dir string) *Filebackend {
	var f Filebackend
	f.dir = dir
	return &f
}

// Filebackend will manage file writing
type Filebackend struct {
	dir string
}

// WriteTo will pass our writer to be writter to
func (fw *Filebackend) WriteTo(key string, fn func(io.Writer) error) (err error) {
	// We decided to make dir here every call to WriteTo to ensure the service is durable.
	// In the off-chance there is someone manually deleting directories, or another service
	// manipulating the same directories. We want to ensure the service continues to work
	// as intended
	if err = os.MkdirAll(fw.dir, 0744); err != nil {
		return
	}

	// Filename is a mixture of the Filebackend directory and the provided key
	filename := path.Join(fw.dir, key)

	var f *os.File
	// Create a file at the given filename
	if f, err = os.Create(filename); err != nil {
		return
	}

	// We want to return this error because this was the first in the chain
	err = fn(f)
	// TODO: Add some Filebackend-level logging to handle this error
	f.Close()

	if err != nil {
		// TODO: Add some Filebackend-level logging to handle this error
		os.Remove(filename)
	}

	return
}
