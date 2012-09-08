/*
Use Swift / Openstack Object Storage / Rackspace cloud files from GO

FIXME need to implement the fixed errors so can distinguish not found etc

FIXME return body close errors

FIXME rename to go-swift to match user agent string

FIXME reconnect on auth error - 403 when token expires

FIXME implement read all files / containers which uses limit and marker to loop

FIXME make more api compatible with python cloudfiles?

FIXME timeout?

Retry operations on timeout / network errors?

FIXME put USER_AGENT and RETRIES into Connection

Make Connection thread safe - whenever it is changed take a write lock whenever it is read from a read lock

Remove header returns - not needed?

 Add extra headers field to Connection (for via etc)

Could potentially store response in Connection but would make it thread unsafe

Make errors use an error heirachy then can catch them with a type assertion

 Error(...)
 ObjectCorrupted{ Error }

Make a Debug flag for logging stuff

*/

package swift

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	USER_AGENT      = "goswift/1.0"
	DEFAULT_RETRIES = 3
)

type Connection struct {
	UserName    string
	ApiKey      string
	AuthUrl     string
	storage_url string
	auth_token  string
	tr          *http.Transport
	client      *http.Client
}

type errorMap map[int]error

var (
	// Custom Errors
	AuthorizationFailed = errors.New("Authorization Failed")
	ContainerNotFound   = errors.New("Container Not Found")
	ContainerNotEmpty   = errors.New("Container Not Empty")
	ObjectNotFound      = errors.New("Object Not Found")
	ObjectCorrupted     = errors.New("Object Corrupted")

	// Mappings for authentication errors
	authErrorMap = errorMap{
		401: AuthorizationFailed,
	}

	// Mappings for container errors
	containerErrorMap = errorMap{
		404: ContainerNotFound,
		409: ContainerNotEmpty,
	}

	// Mappings for object errors
	objectErrorMap = errorMap{
		404: ObjectNotFound,
		422: ObjectCorrupted,
	}
)

// Utility function used to check the return from Close in a defer
// statement
func checkClose(c io.Closer, err *error) {
	cerr := c.Close()
	if *err == nil {
		*err = cerr
	}
}

// Check a response for errors and translate into standard errors if necessary
func (c *Connection) parseHeaders(resp *http.Response, errorMap errorMap) error {
	if errorMap != nil {
		if err, ok := errorMap[resp.StatusCode]; ok {
			return err
		}
	}
	// FIXME convert date header here?
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return errors.New(fmt.Sprintf("HTTP Error: %d: %s", resp.StatusCode, resp.Status))
	}
	return nil
}

// Connects to the cloud storage system
func (c *Connection) Authenticate() (err error) {
	if c.tr == nil {
		c.tr = &http.Transport{
		//		TLSClientConfig:    &tls.Config{RootCAs: pool},
		//		DisableCompression: true,
		}
	}
	if c.client == nil {
		c.client = &http.Client{
			//		CheckRedirect: redirectPolicyFunc,
			Transport: c.tr,
		}
	}
	// Flush the keepalives connection - if we are
	// re-authenticating then stuff has gone wrong
	c.tr.CloseIdleConnections()
	var req *http.Request
	req, err = http.NewRequest("GET", c.AuthUrl, nil)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", USER_AGENT)
	req.Header.Set("X-Auth-Key", c.ApiKey)
	req.Header.Set("X-Auth-User", c.UserName)
	var resp *http.Response
	resp, err = c.client.Do(req)
	if err != nil {
		return
	}
	defer func() {
		checkClose(resp.Body, &err)
		// Flush the auth connection - we don't want to keep
		// it open if keepalives were enabled
		c.tr.CloseIdleConnections()
	}()
	if err = c.parseHeaders(resp, authErrorMap); err != nil {
		return
	}
	c.storage_url = resp.Header.Get("X-Storage-Url")
	c.auth_token = resp.Header.Get("X-Auth-Token")
	if !c.Authenticated() {
		return errors.New("Response didn't have storage url and auth token")
	}
	return nil
}

// Removes the authentication
func (c *Connection) UnAuthenticate() {
	c.storage_url = ""
	c.auth_token = ""
}

// A boolean to show if the current connection is authenticated
//
// Doesn't actually check the credentials
func (c *Connection) Authenticated() bool {
	return c.storage_url != "" && c.auth_token != ""
}

// Run a remote command on a the storage url, returns a deferred to receive the result
// operation is GET, HEAD etc
// container is the name of a container
// Any other parameters (if not None) are added to the storage url

// Returns a response or an error.  If response is returned then
// resp.Body.Close() must be called on it, unless noResponse is set in
// which case the body will be closed in this function

// FIXME make noResponse check for 204?

// This will Authenticate if necessary, and re-authenticate if it
// receives a 403 error which means the token has expired

type storageParams struct {
	container   string
	object_name string
	operation   string
	parameters  url.Values
	headers     map[string]string
	errorMap    errorMap
	noResponse  bool
	body        io.Reader
	retries     int
}

func (c *Connection) storage(p storageParams) (resp *http.Response, err error) {
	retries := p.retries
	if retries == 0 {
		retries = DEFAULT_RETRIES
	}
	for {
		if !c.Authenticated() {
			err = c.Authenticate()
			if err != nil {
				return
			}
		}
		url := c.storage_url
		if p.container != "" {
			url += "/" + p.container
			if p.object_name != "" {
				url += "/" + p.object_name
			}
		}
		if p.parameters != nil {
			encoded := p.parameters.Encode()
			if encoded != "" {
				url += "?" + encoded
			}
		}
		var req *http.Request
		req, err = http.NewRequest(p.operation, url, p.body)
		if err != nil {
			return
		}
		if p.headers != nil {
			for k, v := range p.headers {
				req.Header.Add(k, v)
			}
		}
		req.Header.Add("User-Agent", USER_AGENT)
		req.Header.Add("X-Auth-Token", c.auth_token)
		// FIXME body of request?
		resp, err = c.client.Do(req)
		if err != nil {
			return
		}
		// Check to see if token has expired
		if resp.StatusCode == 403 && retries > 0 {
			_ = resp.Body.Close()
			c.UnAuthenticate()
			retries--
		} else {
			break
		}
	}

	if err = c.parseHeaders(resp, p.errorMap); err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	if p.noResponse {
		err = resp.Body.Close()
		if err != nil {
			return nil, err
		}
	}
	return
}

// Read the response into an array of strings
//
// Closes the response when done
func readLines(resp *http.Response) (lines []string, err error) {
	defer checkClose(resp.Body, &err)
	reader := bufio.NewReader(resp.Body)
	buffer := bytes.NewBuffer(make([]byte, 128))
	var part []byte
	var prefix bool
	for {
		if part, prefix, err = reader.ReadLine(); err != nil {
			break
		}
		buffer.Write(part)
		if !prefix {
			lines = append(lines, buffer.String())
			buffer.Reset()
		}
	}
	if err == io.EOF {
		err = nil
	}
	return
}

// Read the response into the json type passed in
//
// Closes the response when done
func readJson(resp *http.Response, result interface{}) (err error) {
	defer checkClose(resp.Body, &err)
	decoder := json.NewDecoder(resp.Body)
	return decoder.Decode(result)
}

/* ------------------------------------------------------------ */

type ListContainersOpts struct {
	Limit  int    // For an integer value n, limits the number of results to at most n values.
	Marker string // Given a string value x, return object names greater in value than the specified marker.
}

func (opts *ListContainersOpts) parse() url.Values {
	v := url.Values{}
	if opts != nil {
		if opts.Limit > 0 {
			v.Set("limit", strconv.Itoa(opts.Limit))
		}
		if opts.Marker != "" {
			v.Set("marker", opts.Marker)
		}
	}
	return v
}

// Return a list of names of containers
//
//            ['test', 'test2']

func (c *Connection) ListContainers(opts *ListContainersOpts) ([]string, error) {
	v := opts.parse()
	resp, err := c.storage(storageParams{
		operation:  "GET",
		parameters: v,
		errorMap:   containerErrorMap,
	})
	if err != nil {
		return nil, err
	}
	return readLines(resp)
}

// Information about a container
type ContainerInfo struct {
	Name  string // Name of the container
	Count int64  // Number of objects in the container
	Bytes int64  // Total number of bytes used in the container
}

// Return a list of structures with container info
// 
//             [{u'bytes': 315575604, u'count': 1015, u'name': u'test'},
//              {u'bytes': 0, u'count': 1, u'name': u'test2'}]

func (c *Connection) ListContainersInfo(opts *ListContainersOpts) ([]ContainerInfo, error) {
	v := opts.parse()
	v.Set("format", "json")
	resp, err := c.storage(storageParams{
		operation:  "GET",
		parameters: v,
		errorMap:   containerErrorMap,
	})
	if err != nil {
		return nil, err
	}
	var containers []ContainerInfo
	err = readJson(resp, &containers)
	return containers, err
}

/* ------------------------------------------------------------ */

type ListObjectsOpts struct {
	Limit     int    // For an integer value n, limits the number of results to at most n values.
	Marker    string // Given a string value x, return object names greater in value than the  specified marker.
	Prefix    string // For a string value x, causes the results to be limited to object names beginning with the substring x.
	Path      string // For a string value x, return the object names nested in the pseudo path
	Delimiter rune   // For a character c, return all the object names nested in the container
}

func (opts *ListObjectsOpts) parse() url.Values {
	v := url.Values{}
	if opts != nil {
		if opts.Limit > 0 {
			v.Set("limit", strconv.Itoa(opts.Limit))
		}
		if opts.Marker != "" {
			v.Set("marker", opts.Marker)
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
	}
	return v
}

// Return a list of names of containers
//
//            ['test', 'test2']

func (c *Connection) ListObjects(container string, opts *ListObjectsOpts) ([]string, error) {
	v := opts.parse()
	resp, err := c.storage(storageParams{
		container:  container,
		operation:  "GET",
		parameters: v,
		errorMap:   containerErrorMap,
	})
	if err != nil {
		return nil, err
	}
	return readLines(resp)
}

// Information about a container
type ObjectInfo struct {
	Name         string `json:"name"`          // object name
	ContentType  string `json:"content_type"`  // eg application/directory
	Bytes        int64  `json:"bytes"`         // size in bytes
	LastModified string `json:"last_modified"` // Last modified time, eg '2011-06-30T08:20:47.736680'
	Hash         string `json:"hash"`          // MD5 hash, eg "d41d8cd98f00b204e9800998ecf8427e"
}

// Return a list of structures with container info
// 
//             [{u'bytes': 315575604, u'count': 1015, u'name': u'test'},
//              {u'bytes': 0, u'count': 1, u'name': u'test2'}]

func (c *Connection) ListObjectsInfo(container string, opts *ListObjectsOpts) ([]ObjectInfo, error) {
	v := opts.parse()
	v.Set("format", "json")
	resp, err := c.storage(storageParams{
		container:  container,
		operation:  "GET",
		parameters: v,
		errorMap:   containerErrorMap,
	})
	if err != nil {
		return nil, err
	}
	var containers []ObjectInfo
	err = readJson(resp, &containers)
	// FIXME convert the dates!
	return containers, err
}

type AccountInfo struct {
	BytesUsed  int64 // total number of bytes used
	Containers int64 // total number of containers
	Objects    int64 // total number of objects
}

// Helper function to decode int64 from header
func getInt64FromHeader(resp *http.Response, header string) (result int64, err error) {
	value := resp.Header.Get(header)
	result, err = strconv.ParseInt(value, 10, 64)
	if err != nil {
		err = errors.New(fmt.Sprintf("Bad Header '%s': '%s': %s", header, value, err))
	}
	return
}

// Return info about the account
//
// {'bytes_used': 316598182, 'container_count': 4, 'object_count': 1433}
func (c *Connection) AccountInfo() (info AccountInfo, err error) {
	var resp *http.Response
	resp, err = c.storage(storageParams{
		operation:  "HEAD",
		errorMap:   containerErrorMap,
		noResponse: true,
	})
	if err != nil {
		return
	}
	// Parse the headers into a dict
	//
	//    {'Accept-Ranges': 'bytes',
	//     'Content-Length': '0',
	//     'Date': 'Tue, 05 Jul 2011 16:37:06 GMT',
	//     'X-Account-Bytes-Used': '316598182',
	//     'X-Account-Container-Count': '4',
	//     'X-Account-Object-Count': '1433'}
	// FIXME very wordy
	if info.BytesUsed, err = getInt64FromHeader(resp, "X-Account-Bytes-Used"); err != nil {
		return
	}
	if info.Containers, err = getInt64FromHeader(resp, "X-Account-Container-Count"); err != nil {
		return
	}
	if info.Objects, err = getInt64FromHeader(resp, "X-Account-Object-Count"); err != nil {
		return
	}
	return
}

// FIXME Make a container struct so these could be methods on it?

// Create the container.  No error is returned if it already exists.
func (c *Connection) CreateContainer(container string) error {
	_, err := c.storage(storageParams{
		container:  container,
		operation:  "PUT",
		errorMap:   containerErrorMap,
		noResponse: true,
	})
	return err
}

// Delete the container. May return ContainerDoesNotExist or ContainerNotEmpty
func (c *Connection) DeleteContainer(container string) error {
	_, err := c.storage(storageParams{
		container:  container,
		operation:  "DELETE",
		errorMap:   containerErrorMap,
		noResponse: true,
	})
	return err
}

// Returns info about a single container
func (c *Connection) ContainerInfo(container string) (info ContainerInfo, err error) {
	var resp *http.Response
	resp, err = c.storage(storageParams{
		container:  container,
		operation:  "HEAD",
		errorMap:   containerErrorMap,
		noResponse: true,
	})
	if err != nil {
		return
	}
	// FIXME wordy
	// Parse the headers into the struct
	info.Name = container
	if info.Bytes, err = getInt64FromHeader(resp, "X-Container-Bytes-Used"); err != nil {
		return
	}
	if info.Count, err = getInt64FromHeader(resp, "X-Container-Object-Count"); err != nil {
		return
	}
	return
}

// ------------------------------------------------------------

// Create or update the path in the container from contents
// 
// contents should be an open io.Reader which will have all its contents read
// FIXME nil?
// 
// Returns the headers of the response
// 
// If check_md5 is True then it will calculate the md5sum of the
// file as it is being uploaded and check it against that
// returned from the server.  If it is wrong then it will raise ObjectCorrupted
// 
// If md5 is set the it will be sent to the server which will
// check the md5 itself after the upload, and will raise
// ObjectCorrupted if it is incorrect.
// 
// If contentType is set it will be used, otherwise one will be
// guessed from the name using the mimetypes module FIXME

// FIXME I think this will do chunked transfer since we aren't providing a content length

func (c *Connection) CreateObject(container string, objectName string, contents io.Reader, checkMd5 bool, Md5 string, contentType string) (resp *http.Response, err error) {
	if contentType == "" {
		// http.DetectContentType FIXME
		contentType = "application/octet-stream" // FIXME
	}
	// Meta stuff
	extra_headers := map[string]string{
		"Content-Type": contentType,
	}
	if Md5 != "" {
		extra_headers["Etag"] = Md5
		checkMd5 = false // the server will do it
	}
	hash := md5.New()
	var body io.Reader = contents
	if checkMd5 {
		body = io.TeeReader(contents, hash)
	}
	resp, err = c.storage(storageParams{
		container:   container,
		object_name: objectName,
		operation:   "PUT",
		headers:     extra_headers,
		body:        body,
		noResponse:  true,
		errorMap:    objectErrorMap,
	})
	if err != nil {
		return
	}
	if checkMd5 {
		md5 := strings.ToLower(resp.Header.Get("Etag"))
		body_md5 := fmt.Sprintf("%x", hash.Sum(nil))
		if md5 != body_md5 {
			err = ObjectCorrupted
			return
		}
	}
	return
}

// Create an object from a []byte
// This is a simplified interface which checks the MD5 and doesn't return the response
func (c *Connection) CreateObjectBytes(container string, objectName string, contents []byte, contentType string) (err error) {
	buf := bytes.NewBuffer(contents)
	_, err = c.CreateObject(container, objectName, buf, true, "", contentType)
	return
}

// Create an object from a string
// This is a simplified interface which checks the MD5 and doesn't return the response
func (c *Connection) CreateObjectString(container string, objectName string, contents string, contentType string) (err error) {
	buf := strings.NewReader(contents)
	_, err = c.CreateObject(container, objectName, buf, true, "", contentType)
	return
}

// Get the object into the io.Writer contents
// 
// Returns the headers of the response
// 
// If checkMd5 is true then it will calculate the md5sum of the file
// as it is being received and check it against that returned from the
// server.  If it is wrong then it will return ObjectCorrupted

func (c *Connection) GetObject(container string, objectName string, contents io.Writer, checkMd5 bool) (resp *http.Response, err error) {
	// FIXME content-type
	resp, err = c.storage(storageParams{
		container:   container,
		object_name: objectName,
		operation:   "GET",
		errorMap:    objectErrorMap,
	})
	if err != nil {
		return
	}
	defer checkClose(resp.Body, &err)
	hash := md5.New()
	var body io.Writer = contents
	if checkMd5 {
		body = io.MultiWriter(contents, hash)
	}
	var written int64
	written, err = io.Copy(body, resp.Body)
	if err != nil {
		return
	}

	// Check the MD5 sum if requested
	if checkMd5 {
		md5 := strings.ToLower(resp.Header.Get("Etag"))
		body_md5 := fmt.Sprintf("%x", hash.Sum(nil))
		if md5 != body_md5 {
			err = ObjectCorrupted
			return
		}
	}

	// Check to see we wrote the correct number of bytes
	if resp.Header.Get("Content-Length") != "" {
		var object_length int64
		object_length, err = getInt64FromHeader(resp, "Content-Length")
		if err != nil {
			return
		}
		if object_length != written {
			err = ObjectCorrupted
			return
		}
	}

	return
}

// Return an object as a []byte
// This is a simplified interface which checks the MD5 and doesn't return the response
func (c *Connection) GetObjectBytes(container string, objectName string) (contents []byte, err error) {
	var buf bytes.Buffer
	_, err = c.GetObject(container, objectName, &buf, true)
	contents = buf.Bytes()
	return
}

// Return an object as a string
// This is a simplified interface which checks the MD5 and doesn't return the response
func (c *Connection) GetObjectString(container string, objectName string) (contents string, err error) {
	var buf bytes.Buffer
	_, err = c.GetObject(container, objectName, &buf, true)
	contents = buf.String()
	return
}

// Delete the object.  Calls errback if it doesn't exist with
// ObjectDoesNotExist
func (c *Connection) DeleteObject(container string, objectName string) error {
	_, err := c.storage(storageParams{
		container:   container,
		object_name: objectName,
		operation:   "DELETE",
		errorMap:    objectErrorMap,
	})
	return err
}
