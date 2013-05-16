package swift

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

// ObjectOpts is options for Objects() and ObjectNames()
type ObjectsOpts struct {
	Limit     int     // For an integer value n, limits the number of results to at most n values.
	Marker    string  // Given a string value x, return object names greater in value than the  specified marker.
	EndMarker string  // Given a string value x, return object names less in value than the specified marker
	Prefix    string  // For a string value x, causes the results to be limited to object names beginning with the substring x.
	Path      string  // For a string value x, return the object names nested in the pseudo path
	Delimiter rune    // For a character c, return all the object names nested in the container
	Headers   Headers // Any additional HTTP headers - can be nil
}

// parse reads values out of ObjectsOpts
func (opts *ObjectsOpts) parse() (url.Values, Headers) {
	v := url.Values{}
	var h Headers
	if opts != nil {
		if opts.Limit > 0 {
			v.Set("limit", strconv.Itoa(opts.Limit))
		}
		if opts.Marker != "" {
			v.Set("marker", opts.Marker)
		}
		if opts.EndMarker != "" {
			v.Set("end_marker", opts.EndMarker)
		}
		if opts.Prefix != "" {
			v.Set("prefix", opts.Prefix)
		}
		if opts.Path != "" {
			v.Set("path", opts.Path)
		}
		if opts.Delimiter != 0 {
			v.Set("delimiter", string(opts.Delimiter))
		}
		h = opts.Headers
	}
	return v, h
}

// ObjectNames returns a slice of names of objects in a given container.
func (c *Connection) ObjectNames(container string, opts *ObjectsOpts) ([]string, error) {
	v, h := opts.parse()
	resp, _, err := c.storage(RequestOpts{
		Container:  container,
		Operation:  "GET",
		Parameters: v,
		ErrorMap:   ContainerErrorMap,
		Headers:    h,
	})
	if err != nil {
		return nil, err
	}
	return readLines(resp)
}

// Object contains information about an object
type Object struct {
	Name               string    `json:"name"`          // object name
	ContentType        string    `json:"content_type"`  // eg application/directory
	Bytes              int64     `json:"bytes"`         // size in bytes
	ServerLastModified string    `json:"last_modified"` // Last modified time, eg '2011-06-30T08:20:47.736680' as a string supplied by the server
	LastModified       time.Time // Last modified time converted to a time.Time
	Hash               string    `json:"hash"` // MD5 hash, eg "d41d8cd98f00b204e9800998ecf8427e"
	PseudoDirectory    bool      // Set when using delimiter to show that this directory object does not really exist
	SubDir             string    `json:"subdir"` // returned only when using delimiter to mark "pseudo directories"
}

// Objects returns a slice of Object with information about each
// object in the container.
//
// If Delimiter is set in the opts then PseudoDirectory may be set,
// with ContentType 'application/directory'.  These are not real
// objects but represent directories of objects which haven't had an
// object created for them.
func (c *Connection) Objects(container string, opts *ObjectsOpts) ([]Object, error) {
	v, h := opts.parse()
	v.Set("format", "json")
	resp, _, err := c.storage(RequestOpts{
		Container:  container,
		Operation:  "GET",
		Parameters: v,
		ErrorMap:   ContainerErrorMap,
		Headers:    h,
	})
	if err != nil {
		return nil, err
	}
	var objects []Object
	err = readJson(resp, &objects)
	// Convert Pseudo directories and dates
	for i := range objects {
		object := &objects[i]
		if object.SubDir != "" {
			object.Name = object.SubDir
			object.PseudoDirectory = true
			object.ContentType = "application/directory"
		}
		if object.ServerLastModified != "" {
			// 2012-11-11T14:49:47.887250
			//
			// Remove fractional seconds if present. This
			// then keeps it consistent with Object
			// which can only return timestamps accurate
			// to 1 second
			//
			// The TimeFormat will parse fractional
			// seconds if desired though
			datetime := strings.SplitN(object.ServerLastModified, ".", 2)[0]
			object.LastModified, err = time.Parse(TimeFormat, datetime)
			if err != nil {
				return nil, err
			}
		}
	}
	return objects, err
}

// objectsAllOpts makes a copy of opts if set or makes a new one and
// overrides Limit and Marker
func objectsAllOpts(opts *ObjectsOpts, Limit int) *ObjectsOpts {
	var newOpts ObjectsOpts
	if opts != nil {
		newOpts = *opts
	}
	if newOpts.Limit == 0 {
		newOpts.Limit = Limit
	}
	newOpts.Marker = ""
	return &newOpts
}

// A closure defined by the caller to iterate through all objects
//
// Call Objects or ObjectNames from here with the *ObjectOpts passed in
//
// Do whatever is required with the results then return them
type ObjectsWalkFn func(*ObjectsOpts) (interface{}, error)

// ObjectsWalk is uses to iterate through all the objects in chunks as
// returned by Objects or ObjectNames using the Marker and Limit
// parameters in the ObjectsOpts.
//
// Pass in a closure `walkFn` which calls Objects or ObjectNames with
// the *ObjectsOpts passed to it and does something with the results.
//
// Errors will be returned from this function
//
// It has a default Limit parameter but you may pass in your own
func (c *Connection) ObjectsWalk(container string, opts *ObjectsOpts, walkFn ObjectsWalkFn) error {
	opts = objectsAllOpts(opts, allObjectsChanLimit)
	for {
		objects, err := walkFn(opts)
		if err != nil {
			return err
		}
		var n int
		var last string
		switch objects := objects.(type) {
		case []string:
			n = len(objects)
			if n > 0 {
				last = objects[len(objects)-1]
			}
		case []Object:
			n = len(objects)
			if n > 0 {
				last = objects[len(objects)-1].Name
			}
		default:
			panic("Unknown type returned to ObjectsWalk")
		}
		if n < opts.Limit {
			break
		}
		opts.Marker = last
	}
	return nil
}

// ObjectsAll is like Objects but it returns an unlimited number of Objects in a slice
//
// It calls Objects multiple times using the Marker parameter
func (c *Connection) ObjectsAll(container string, opts *ObjectsOpts) ([]Object, error) {
	objects := make([]Object, 0)
	err := c.ObjectsWalk(container, opts, func(opts *ObjectsOpts) (interface{}, error) {
		newObjects, err := c.Objects(container, opts)
		if err == nil {
			objects = append(objects, newObjects...)
		}
		return newObjects, err
	})
	return objects, err
}

// ObjectNamesAll is like ObjectNames but it returns all the Objects
//
// It calls ObjectNames multiple times using the Marker parameter
//
// It has a default Limit parameter but you may pass in your own
func (c *Connection) ObjectNamesAll(container string, opts *ObjectsOpts) ([]string, error) {
	objects := make([]string, 0)
	err := c.ObjectsWalk(container, opts, func(opts *ObjectsOpts) (interface{}, error) {
		newObjects, err := c.ObjectNames(container, opts)
		if err == nil {
			objects = append(objects, newObjects...)
		}
		return newObjects, err
	})
	return objects, err
}

// Account contains information about this account.
type Account struct {
	BytesUsed  int64 // total number of bytes used
	Containers int64 // total number of containers
	Objects    int64 // total number of objects
}

/* ------------------------------------------------------------ */

// ObjectCreateFile represents a swift object open for writing
type ObjectCreateFile struct {
	checkHash  bool           // whether we are checking the hash
	pipeReader *io.PipeReader // pipe for the caller to use
	pipeWriter *io.PipeWriter
	hash       hash.Hash      // hash being build up as we go along
	done       chan bool      // signals when the upload has finished
	resp       *http.Response // valid when done has signalled
	err        error          // ditto
	headers    Headers        // ditto
}

// Write bytes to the object - see io.Writer
func (file *ObjectCreateFile) Write(p []byte) (n int, err error) {
	if file.checkHash {
		_, _ = file.hash.Write(p)
	}
	return file.pipeWriter.Write(p)
}

// Close the object and checks the md5sum if it was required.
//
// Also returns any other errors from the server (eg container not
// found) so it is very important to check the errors on this method.
func (file *ObjectCreateFile) Close() error {
	// Close the body
	err := file.pipeWriter.Close()
	if err != nil {
		return err
	}

	// Wait for the HTTP operation to complete
	<-file.done

	// Check errors
	if file.err != nil {
		return file.err
	}
	if file.checkHash {
		receivedMd5 := strings.ToLower(file.headers["Etag"])
		calculatedMd5 := fmt.Sprintf("%x", file.hash.Sum(nil))
		if receivedMd5 != calculatedMd5 {
			return ObjectCorrupted
		}
	}
	return nil
}

// Check it satisfies the interface
var _ io.WriteCloser = &ObjectCreateFile{}

// objectPutHeaders create a set of headers for a PUT
//
// It guesses the contentType from the objectName if it isn't set
//
// checkHash may be changed
func objectPutHeaders(objectName string, checkHash *bool, Hash string, contentType string, h Headers) Headers {
	if contentType == "" {
		contentType = mime.TypeByExtension(path.Ext(objectName))
		if contentType == "" {
			contentType = "application/octet-stream"
		}
	}
	// Meta stuff
	extraHeaders := map[string]string{
		"Content-Type": contentType,
	}
	for key, value := range h {
		extraHeaders[key] = value
	}
	if Hash != "" {
		extraHeaders["Etag"] = Hash
		*checkHash = false // the server will do it
	}
	return extraHeaders
}

// ObjectCreate creates or updates the object in the container.  It
// returns an io.WriteCloser you should write the contents to.  You
// MUST call Close() on it and you MUST check the error return from
// Close().
//
// If checkHash is True then it will calculate the MD5 Hash of the
// file as it is being uploaded and check it against that returned
// from the server.  If it is wrong then it will return
// ObjectCorrupted on Close()
//
// If you know the MD5 hash of the object ahead of time then set the
// Hash parameter and it will be sent to the server (as an Etag
// header) and the server will check the MD5 itself after the upload,
// and this will return ObjectCorrupted on Close() if it is incorrect.
//
// If you don't want any error protection (not recommended) then set
// checkHash to false and Hash to "".
//
// If contentType is set it will be used, otherwise one will be
// guessed from objectName using mime.TypeByExtension
func (c *Connection) ObjectCreate(container string, objectName string, checkHash bool, Hash string, contentType string, h Headers) (file *ObjectCreateFile, err error) {
	extraHeaders := objectPutHeaders(objectName, &checkHash, Hash, contentType, h)
	pipeReader, pipeWriter := io.Pipe()
	file = &ObjectCreateFile{
		hash:       md5.New(),
		checkHash:  checkHash,
		pipeReader: pipeReader,
		pipeWriter: pipeWriter,
		done:       make(chan bool),
	}
	// Run the PUT in the background piping it data
	go func() {
		file.resp, file.headers, file.err = c.storage(RequestOpts{
			Container:  container,
			ObjectName: objectName,
			Operation:  "PUT",
			Headers:    extraHeaders,
			Body:       pipeReader,
			NoResponse: true,
			ErrorMap:   objectErrorMap,
		})
		// Signal finished
		file.done <- true
	}()
	return
}

// ObjectPut creates or updates the path in the container from
// contents.  contents should be an open io.Reader which will have all
// its contents read.
//
// This is a low level interface.
//
// If checkHash is True then it will calculate the MD5 Hash of the
// file as it is being uploaded and check it against that returned
// from the server.  If it is wrong then it will return
// ObjectCorrupted.
//
// If you know the MD5 hash of the object ahead of time then set the
// Hash parameter and it will be sent to the server (as an Etag
// header) and the server will check the MD5 itself after the upload,
// and this will return ObjectCorrupted if it is incorrect.
//
// If you don't want any error protection (not recommended) then set
// checkHash to false and Hash to "".
//
// If contentType is set it will be used, otherwise one will be
// guessed from objectName using mime.TypeByExtension
func (c *Connection) ObjectPut(container string, objectName string, contents io.Reader, checkHash bool, Hash string, contentType string, h Headers) (headers Headers, err error) {
	extraHeaders := objectPutHeaders(objectName, &checkHash, Hash, contentType, h)
	hash := md5.New()
	var body io.Reader = contents
	if checkHash {
		body = io.TeeReader(contents, hash)
	}
	_, headers, err = c.storage(RequestOpts{
		Container:  container,
		ObjectName: objectName,
		Operation:  "PUT",
		Headers:    extraHeaders,
		Body:       body,
		NoResponse: true,
		ErrorMap:   objectErrorMap,
	})
	if err != nil {
		return
	}
	if checkHash {
		receivedMd5 := strings.ToLower(headers["Etag"])
		calculatedMd5 := fmt.Sprintf("%x", hash.Sum(nil))
		if receivedMd5 != calculatedMd5 {
			err = ObjectCorrupted
			return
		}
	}
	return
}

// ObjectPutBytes creates an object from a []byte in a container.
//
// This is a simplified interface which checks the MD5.
func (c *Connection) ObjectPutBytes(container string, objectName string, contents []byte, contentType string) (err error) {
	buf := bytes.NewBuffer(contents)
	_, err = c.ObjectPut(container, objectName, buf, true, "", contentType, nil)
	return
}

// ObjectPutString creates an object from a string in a container.
//
// This is a simplified interface which checks the MD5
func (c *Connection) ObjectPutString(container string, objectName string, contents string, contentType string) (err error) {
	buf := strings.NewReader(contents)
	_, err = c.ObjectPut(container, objectName, buf, true, "", contentType, nil)
	return
}

// ObjectOpenFile represents a swift object open for reading
type ObjectOpenFile struct {
	connection *Connection    // stored copy of Connection used in Open
	container  string         // stored copy of container used in Open
	objectName string         // stored copy of objectName used in Open
	headers    Headers        // stored copy of headers used in Open
	resp       *http.Response // http connection
	body       io.Reader      // read data from this
	checkHash  bool           // true if checking MD5
	hash       hash.Hash      // currently accumulating MD5
	bytes      int64          // number of bytes read on this connection
	eof        bool           // whether we have read end of file
	pos        int64          // current position when reading
	lengthOk   bool           // whether length is valid
	length     int64          // length of the object if read
	seeked     bool           // whether we have seeked this file or not
}

// Read bytes from the object - see io.Reader
func (file *ObjectOpenFile) Read(p []byte) (n int, err error) {
	n, err = file.body.Read(p)
	file.bytes += int64(n)
	file.pos += int64(n)
	if err == io.EOF {
		file.eof = true
	}
	return
}

// Seek sets the offset for the next Read to offset, interpreted
// according to whence: 0 means relative to the origin of the file, 1
// means relative to the current offset, and 2 means relative to the
// end. Seek returns the new offset and an Error, if any.
//
// Seek uses HTTP Range headers which, if the file pointer is moved,
// will involve reopening the HTTP connection.
//
// Note that you can't seek to the end of a file or beyond; HTTP Range
// requests don't support the file pointer being outside the data,
// unlike os.File
//
// Seek(0, 1) will return the current file pointer.
func (file *ObjectOpenFile) Seek(offset int64, whence int) (newPos int64, err error) {
	switch whence {
	case 0: // relative to start
		newPos = offset
	case 1: // relative to current
		newPos = file.pos + offset
	case 2: // relative to end
		if !file.lengthOk {
			return file.pos, newError(0, "Length of file unknown so can't seek from end")
		}
		newPos = file.length + offset
	default:
		panic("Unknown whence in ObjectOpenFile.Seek")
	}
	// If at correct position (quite likely), do nothing
	if newPos == file.pos {
		return
	}
	// Close the file...
	file.seeked = true
	err = file.Close()
	if err != nil {
		return
	}
	// ...and re-open with a Range header
	if file.headers == nil {
		file.headers = Headers{}
	}
	if newPos > 0 {
		file.headers["Range"] = fmt.Sprintf("bytes=%d-", newPos)
	} else {
		delete(file.headers, "Range")
	}
	newFile, _, err := file.connection.ObjectOpen(file.container, file.objectName, false, file.headers)
	if err != nil {
		return
	}
	// Update the file
	file.resp = newFile.resp
	file.body = newFile.body
	file.checkHash = false
	file.pos = newPos
	return
}

// Close the object and checks the length and md5sum if it was
// required and all the object was read
func (file *ObjectOpenFile) Close() (err error) {
	// Close the body at the end
	defer checkClose(file.resp.Body, &err)

	// If not end of file or seeked then can't check anything
	if !file.eof || file.seeked {
		return
	}

	// Check the MD5 sum if requested
	if file.checkHash {
		receivedMd5 := strings.ToLower(file.resp.Header.Get("Etag"))
		calculatedMd5 := fmt.Sprintf("%x", file.hash.Sum(nil))
		if receivedMd5 != calculatedMd5 {
			err = ObjectCorrupted
			return
		}
	}

	// Check to see we read the correct number of bytes
	if file.lengthOk && file.length != file.bytes {
		err = ObjectCorrupted
		return
	}
	return
}

// Check it satisfies the interfaces
var _ io.ReadCloser = &ObjectOpenFile{}
var _ io.Seeker = &ObjectOpenFile{}

// ObjectOpen returns an ObjectOpenFile for reading the contents of
// the object.  This satisfies the io.ReadCloser and the io.Seeker
// interfaces.
//
// You must call Close() on contents when finished
//
// Returns the headers of the response.
//
// If checkHash is true then it will calculate the md5sum of the file
// as it is being received and check it against that returned from the
// server.  If it is wrong then it will return ObjectCorrupted. It
// will also check the length returned. No checking will be done if
// you don't read all the contents.
//
// headers["Content-Type"] will give the content type if desired.
func (c *Connection) ObjectOpen(container string, objectName string, checkHash bool, h Headers) (file *ObjectOpenFile, headers Headers, err error) {
	var resp *http.Response
	resp, headers, err = c.storage(RequestOpts{
		Container:  container,
		ObjectName: objectName,
		Operation:  "GET",
		ErrorMap:   objectErrorMap,
		Headers:    h,
	})
	if err != nil {
		return
	}
	file = &ObjectOpenFile{
		connection: c,
		container:  container,
		objectName: objectName,
		headers:    h,
		resp:       resp,
		checkHash:  checkHash,
		body:       resp.Body,
	}
	if checkHash {
		file.hash = md5.New()
		file.body = io.TeeReader(resp.Body, file.hash)
	}
	// Read Content-Length
	file.length, err = getInt64FromHeader(resp, "Content-Length")
	file.lengthOk = (err == nil)
	return
}

// ObjectGet gets the object into the io.Writer contents.
//
// Returns the headers of the response.
//
// If checkHash is true then it will calculate the md5sum of the file
// as it is being received and check it against that returned from the
// server.  If it is wrong then it will return ObjectCorrupted.
//
// headers["Content-Type"] will give the content type if desired.
func (c *Connection) ObjectGet(container string, objectName string, contents io.Writer, checkHash bool, h Headers) (headers Headers, err error) {
	file, headers, err := c.ObjectOpen(container, objectName, checkHash, h)
	if err != nil {
		return
	}
	defer checkClose(file, &err)
	_, err = io.Copy(contents, file)
	return
}

// ObjectGetBytes returns an object as a []byte.
//
// This is a simplified interface which checks the MD5
func (c *Connection) ObjectGetBytes(container string, objectName string) (contents []byte, err error) {
	var buf bytes.Buffer
	_, err = c.ObjectGet(container, objectName, &buf, true, nil)
	contents = buf.Bytes()
	return
}

// ObjectGetString returns an object as a string.
//
// This is a simplified interface which checks the MD5
func (c *Connection) ObjectGetString(container string, objectName string) (contents string, err error) {
	var buf bytes.Buffer
	_, err = c.ObjectGet(container, objectName, &buf, true, nil)
	contents = buf.String()
	return
}

// ObjectDelete deletes the object.
//
// May return ObjectDoesNotExist if the object isn't found
func (c *Connection) ObjectDelete(container string, objectName string) error {
	_, _, err := c.storage(RequestOpts{
		Container:  container,
		ObjectName: objectName,
		Operation:  "DELETE",
		ErrorMap:   objectErrorMap,
	})
	return err
}

// Object returns info about a single object including any metadata in the header.
//
// May return ObjectNotFound.
//
// Use headers.ObjectMetadata() to read the metadata in the Headers.
func (c *Connection) Object(container string, objectName string) (info Object, headers Headers, err error) {
	var resp *http.Response
	resp, headers, err = c.storage(RequestOpts{
		Container:  container,
		ObjectName: objectName,
		Operation:  "HEAD",
		ErrorMap:   objectErrorMap,
		NoResponse: true,
	})
	if err != nil {
		return
	}
	// Parse the headers into the struct
	// HTTP/1.1 200 OK
	// Date: Thu, 07 Jun 2010 20:59:39 GMT
	// Server: Apache
	// Last-Modified: Fri, 12 Jun 2010 13:40:18 GMT
	// ETag: 8a964ee2a5e88be344f36c22562a6486
	// Content-Length: 512000
	// Content-Type: text/plain; charset=UTF-8
	// X-Object-Meta-Meat: Bacon
	// X-Object-Meta-Fruit: Bacon
	// X-Object-Meta-Veggie: Bacon
	// X-Object-Meta-Dairy: Bacon
	info.Name = objectName
	info.ContentType = resp.Header.Get("Content-Type")
	if info.Bytes, err = getInt64FromHeader(resp, "Content-Length"); err != nil {
		return
	}
	info.ServerLastModified = resp.Header.Get("Last-Modified")
	if info.LastModified, err = time.Parse(http.TimeFormat, info.ServerLastModified); err != nil {
		return
	}
	info.Hash = resp.Header.Get("Etag")
	return
}

// ObjectUpdate adds, replaces or removes object metadata.
//
// Add or Update keys by mentioning them in the Metadata.  Use
// Metadata.ObjectHeaders and Headers.ObjectMetadata to convert your
// Metadata to and from normal HTTP headers.
//
// This removes all metadata previously added to the object and
// replaces it with that passed in so to delete keys, just don't
// mention them the headers you pass in.
//
// Object metadata can only be read with Object() not with Objects().
//
// This can also be used to set headers not already assigned such as
// X-Delete-At or X-Delete-After for expiring objects.
//
// You cannot use this to change any of the object's other headers
// such as Content-Type, ETag, etc.
//
// Refer to copying an object when you need to update metadata or
// other headers such as Content-Type or CORS headers.
//
// May return ObjectNotFound.
func (c *Connection) ObjectUpdate(container string, objectName string, h Headers) error {
	_, _, err := c.storage(RequestOpts{
		Container:  container,
		ObjectName: objectName,
		Operation:  "POST",
		ErrorMap:   objectErrorMap,
		NoResponse: true,
		Headers:    h,
	})
	return err
}

// ObjectCopy does a server side copy of an object to a new position
//
// All metadata is preserved.  If metadata is set in the headers then
// it overrides the old metadata on the copied object.
//
// The destination container must exist before the copy.
//
// You can use this to copy an object to itself - this is the only way
// to update the content type of an object.
func (c *Connection) ObjectCopy(srcContainer string, srcObjectName string, dstContainer string, dstObjectName string, h Headers) (headers Headers, err error) {
	// Meta stuff
	extraHeaders := map[string]string{
		"Destination": dstContainer + "/" + dstObjectName,
	}
	for key, value := range h {
		extraHeaders[key] = value
	}
	_, headers, err = c.storage(RequestOpts{
		Container:  srcContainer,
		ObjectName: srcObjectName,
		Operation:  "COPY",
		ErrorMap:   objectErrorMap,
		NoResponse: true,
		Headers:    extraHeaders,
	})
	return
}

// ObjectMove does a server side move of an object to a new position
//
// This is a convenience method which calls ObjectCopy then ObjectDelete
//
// All metadata is preserved.
//
// The destination container must exist before the copy.
func (c *Connection) ObjectMove(srcContainer string, srcObjectName string, dstContainer string, dstObjectName string) (err error) {
	_, err = c.ObjectCopy(srcContainer, srcObjectName, dstContainer, dstObjectName, nil)
	if err != nil {
		return
	}
	return c.ObjectDelete(srcContainer, srcObjectName)
}

// ObjectUpdateContentType updates the content type of an object
//
// This is a convenience method which calls ObjectCopy
//
// All other metadata is preserved.
func (c *Connection) ObjectUpdateContentType(container string, objectName string, contentType string) (err error) {
	h := Headers{"Content-Type": contentType}
	_, err = c.ObjectCopy(container, objectName, container, objectName, h)
	return
}
