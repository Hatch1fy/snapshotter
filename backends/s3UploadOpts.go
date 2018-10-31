package backends

// S3UploadOpts is the set of supported options for s3 uploads
type S3UploadOpts struct {
	CacheControl       string
	ContentDisposition string
	ContentEncoding    string
	ContentLanguage    string
	ContentType        string
	ContentMD5         string
	ACL                string
}

// GetCacheControl will retrieve a string pointer of the cache control value
func (o *S3UploadOpts) GetCacheControl() *string {
	return getStrPtr(o.CacheControl)
}

// GetContentDisposition will retrieve a string pointer of the content disposition value
func (o *S3UploadOpts) GetContentDisposition() *string {
	return getStrPtr(o.ContentDisposition)
}

// GetContentEncoding will retrieve a string pointer of the content encoding value
func (o *S3UploadOpts) GetContentEncoding() *string {
	return getStrPtr(o.ContentEncoding)
}

// GetContentLanguage will retrieve a string pointer of the content language value
func (o *S3UploadOpts) GetContentLanguage() *string {

	return getStrPtr(o.ContentLanguage)
}

// GetContentType will retrieve a string pointer of the content type value
func (o *S3UploadOpts) GetContentType() *string {
	return getStrPtr(o.ContentType)
}

// GetContentMD5 will retrieve a string pointer of the md5 content value
func (o *S3UploadOpts) GetContentMD5() *string {
	return getStrPtr(o.ContentMD5)
}

// GetACL will retrieve a string pointer of the ACL content value
func (o *S3UploadOpts) GetACL() *string {
	return getStrPtr(o.ACL)
}

// getStrPtr will get a string pointer of the provided string
// Note: Unset strings will return a nil pointer
func getStrPtr(str string) *string {
	if len(str) == 0 {
		return nil
	}

	return &str
}
