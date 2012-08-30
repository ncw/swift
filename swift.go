/*
Use Swift / Openstack Object Storage / Rackspace cloud files from GO

FIXME need to implement the fixed errors so can distinguish not found etc

*/

package swift

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	//	"strings"
	"bytes"
	"encoding/json"
)

const (
	USER_AGENT = "goswift/1.0"
)

type Connection struct {
	UserName    string
	ApiKey     string
	AuthUrl     string
	storage_url string
	auth_token  string
}

type errorMap map[int]error
var (
	// Custom Errors
	AuthorizationFailed = errors.New("Authorization Failed")
	ContainerNotFound = errors.New("Container Not Found")
	ContainerNotEmpty = errors.New("Container Not Empty")
	ObjectNotFound = errors.New("Object Not Found")
	ObjectCorrupted = errors.New("Object Corrupted")
	
	// Mappings for authentication errors
	authErrorMap = errorMap{
		401 : AuthorizationFailed,
	}

	// Mappings for container errors
	containerErrorMap = errorMap{
		404 : ContainerNotFound,
		409 : ContainerNotEmpty,
	}

	// Mappings for object errors
	objectErrorMap = errorMap{
		404 : ObjectNotFound,
		422 : ObjectCorrupted,
	}
)	

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
func (c *Connection) Authenticate() error {
	tr := &http.Transport{
	//		TLSClientConfig:    &tls.Config{RootCAs: pool},
	//		DisableCompression: true,
	}
	client := &http.Client{
		//		CheckRedirect: redirectPolicyFunc,
		Transport: tr,
	}
	req, err := http.NewRequest("GET", c.AuthUrl, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", USER_AGENT)
	req.Header.Set("X-Auth-Key", c.ApiKey)
	req.Header.Set("X-Auth-User", c.UserName)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := c.parseHeaders(resp, authErrorMap); err != nil {
		return err
	}
	c.storage_url = resp.Header.Get("X-Storage-Url")
	c.auth_token = resp.Header.Get("X-Auth-Token")
	if !c.Authenticated() {
		return errors.New("Response didn't have storage url and auth token")
	}
	return nil
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
// resp.Body.Close() must be called on it, unless noBody is set in
// which case the body will be closed in this function

type storageParams struct {
	container   string
	object_name string
	operation   string
	parameters  url.Values
	headers     map[string]string
	errorMap    errorMap
	noBody      bool
	// body=None
}

func (c *Connection) storage(p storageParams) (*http.Response, error) {
	if !c.Authenticated() {
		return nil, errors.New("Not logged in")
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
	tr := &http.Transport{
	//		TLSClientConfig:    &tls.Config{RootCAs: pool},
	//		DisableCompression: true,
	}
	client := &http.Client{
		//		CheckRedirect: redirectPolicyFunc,
		Transport: tr,
	}
	req, err := http.NewRequest(p.operation, url, nil)
	if err != nil {
		return nil, err
	}
	if p.headers != nil {
		for k, v := range p.headers {
			req.Header.Add(k, v)
		}
	}
	req.Header.Add("User-Agent", USER_AGENT)
	req.Header.Add("X-Auth-Token", c.auth_token)
	// FIXME extra_headers
	// FIXME body of request?
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if err := c.parseHeaders(resp, p.errorMap); err != nil {
		resp.Body.Close()
		return nil, err
	}
	if p.noBody {
		resp.Body.Close()
	}
	return resp, nil
}

// Read the response into an array of strings
//
// Closes the response when done
func readLines(resp *http.Response) (lines []string, err error) {
	defer resp.Body.Close()
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
func readJson(resp *http.Response, result interface{}) error {
	defer resp.Body.Close()
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
		errorMap: containerErrorMap,
	})
	if err != nil {
		return nil, err
	}
	return readLines(resp)
}

// Information about a container
type ContainerInfo struct {
	Name  string
	Count int64
	Bytes int64
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
		errorMap: containerErrorMap,
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
	Limit int	// For an integer value n, limits the number of results to at most n values.
	Marker string	// Given a string value x, return object names greater in value than the  specified marker.
	Prefix string	// For a string value x, causes the results to be limited to object names beginning with the substring x.
	Path string	// For a string value x, return the object names nested in the pseudo path
	Delimiter rune	// For a character c, return all the object names nested in the container
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
		errorMap: containerErrorMap,
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
	Bytes        int64  `json:"bytes"`	   // size in bytes
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
		errorMap: containerErrorMap,
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
	BytesUsed int64		// total number of bytes used
	Containers int64	// total number of containers
	Objects int64		// total number of objects
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
func (c * Connection) AccountInfo() (info AccountInfo, err error) {
	var resp *http.Response
	resp, err = c.storage(storageParams{
		operation:  "HEAD",
		errorMap: containerErrorMap,
		noBody: true,
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

// Create the container.  No error is returned if it already exists.
func (c *Connection) CreateContainer(container string) error {
	_, err := c.storage(storageParams{
		container:  container,
		operation:  "PUT",
		errorMap: containerErrorMap,
		noBody: true,
	})
	return err
}

// Delete the container. May return ContainerDoesNotExist or ContainerNotEmpty
func (c *Connection) DeleteContainer(container string) error {
	_, err := c.storage(storageParams{
		container:  container,
		operation:  "DELETE",
		errorMap: containerErrorMap,
		noBody: true,
	})
	return err
}
