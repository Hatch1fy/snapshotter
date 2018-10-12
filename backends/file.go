package backends

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// NewFile will return a new instance of File
func NewFile(dir string) *File {
	var f File
	f.dir = dir
	return &f
}

// File will manage file writing
type File struct {
	dir string
}

// WriteTo will pass a writer to the provided function
func (fb *File) WriteTo(key string, fn func(io.Writer) error) (err error) {
	// We decided to make dir here every call to WriteTo to ensure the service is durable.
	// In the off-chance there is someone manually deleting directories, or another service
	// manipulating the same directories. We want to ensure the service continues to work
	// as intended
	if err = os.MkdirAll(fb.dir, 0744); err != nil {
		return
	}

	// Filename is a mixture of the File directory and the provided key
	filename := path.Join(fb.dir, key)

	var f *os.File
	// Create a file at the given filename
	if f, err = os.Create(filename); err != nil {
		return
	}

	// We want to return this error because this was the first in the chain
	err = fn(f)
	f.Close()

	if err != nil {
		// We encountered an error, delete the file
		os.Remove(filename)
	}

	return
}

// ReadFrom will pass a reader to the provided function
func (fb *File) ReadFrom(key string, fn func(io.Reader) error) (err error) {
	var f *os.File
	// Filename is a mixture of the File directory and the provided key
	filename := path.Join(fb.dir, key)
	// Create a file at the given filename
	if f, err = os.Open(filename); err != nil {
		return
	}
	// Defer the closing of the file
	defer f.Close()
	// Call provided func and pass file
	return fn(f)
}

// Delete will delete a key
func (fb *File) Delete(key string) (err error) {
	return os.Remove(filepath.Join(fb.dir, key))
}

// ForEach will iterate through all the keys
func (fb *File) ForEach(prefix, marker string, maxKeys int64, fn ForEachFn) (err error) {
	var cnt int64
	err = filepath.Walk(fb.dir, func(filepath string, info os.FileInfo, ierr error) (err error) {
		if info.IsDir() {
			return
		}

		// Truncate filepath to exclude the directory
		filepath = path.Base(filepath)

		if strings.Index(filepath, prefix) == -1 {
			return
		}

		// Check to see if we've past the marker yet
		if filepath <= marker {
			return
		}

		if err = fn(filepath); err != nil {
			return
		}

		if maxKeys == -1 {
			return
		}

		if cnt++; cnt == maxKeys {
			return Break
		}

		return
	})

	if err == Break {
		err = nil
	}

	return
}

// Next will return the next key
func (fb *File) Next(prefix, marker string) (nextKey string, err error) {
	err = fb.ForEach(prefix, marker, 1, func(key string) (err error) {
		nextKey = key
		return
	})

	if len(nextKey) == 0 {
		err = io.EOF
	}

	return
}

// List will list the available keys
func (fb *File) List(prefix, marker string, maxKeys int64) (keys []string, err error) {
	err = fb.ForEach(prefix, marker, maxKeys, func(key string) (err error) {
		keys = append(keys, key)
		return
	})

	return
}
