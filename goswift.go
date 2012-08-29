/*
Use Swift / Openstack Object Storage / Rackspace cloud files from GO

FIXME need to implement the fixed errors so can distinguish not found etc

*/

package main

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

// curl -v -H 'X-Storage-User: admin:admin' -H 'X-Storage-Pass: admin' http://10.10.10.2:8080/auth/v1.0

type Connection struct {
	username    string
	api_key     string
	authurl     string
	storage_url string
	auth_token  string
}

/*
// Mappings for authentication errors
_auth_error_map = {
    401 : AuthorizationFailed,
}

// Mappings for container errors
_container_error_map = {
    404 : ContainerNotFound,
    409 : ContainerNotEmpty,
}

// Mappings for object errors
_object_error_map = {
    404 : ObjectNotFound,
    422 : ObjectCorrupted,
}
*/

// Check a response for errors and translate into standard errors if necessary
// FIXME error map
func (c *Connection) parseHeaders(resp *http.Response) error {
	// if error_map:
	//     e = error_map.get(response.code)
	//     if e is not None:
	//         raise e()
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
	req, err := http.NewRequest("GET", c.authurl, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", USER_AGENT)
	req.Header.Set("X-Auth-Key", c.api_key)
	req.Header.Set("X-Auth-User", c.username)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := c.parseHeaders(resp); err != nil {
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

// Returns a response or an error.  If response is returned then resp.Body.Close() must be called on it

type storageParams struct {
	container   string
	object_name string
	operation   string
	parameters  url.Values
	headers     map[string]string
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
	if err := c.parseHeaders(resp); err != nil {
		resp.Body.Close()
		return nil, err
	}
	// FIXME must do something with resp.Body.Close
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

// Return a deferred to receive the list of names of containers
//
//    limit     For an integer value n, limits the number of results to at most n values.
//	
//    marker    Given a string value x, return object names greater in value than the specified marker.
//
//            ['test', 'test2']

func (c *Connection) ListContainers(limit int, marker string) ([]string, error) {
	v := url.Values{}
	if limit > 0 {
		v.Set("limit", strconv.Itoa(limit))
	}
	if marker != "" {
		v.Set("marker", marker)
	}
	resp, err := c.storage(storageParams{
		operation:  "GET",
		parameters: v,
	})
	if err != nil {
		return nil, err
	}
	return readLines(resp)
}


// Information about a container
type ContainerInfo struct {
	Name string
	Count int64
	Bytes int64
}

// Return a deferred to receive the list of dictionaries with container info
// 
//     limit     For an integer value n, limits the number of results to at most n values.
// 
//     marker    Given a string value x, return object names greater in value than the
//               specified marker.
// 
//             [{u'bytes': 315575604, u'count': 1015, u'name': u'test'},
//              {u'bytes': 0, u'count': 1, u'name': u'test2'}]

func (c *Connection) ListContainersInfo(limit int, marker string) ([]ContainerInfo, error) {
	// FIXME factor ListConatiners
	v := url.Values{}
	if limit > 0 {
		v.Set("limit", strconv.Itoa(limit))
	}
	if marker != "" {
		v.Set("marker", marker)
	}
	v.Set("format", "json")
	resp, err := c.storage(storageParams{
		operation:  "GET",
		parameters: v,
	})
	if err != nil {
		return nil, err
	}
	var containers []ContainerInfo
	err = readJson(resp, &containers)
	return containers, err
}


func main() {
	c := Connection{
		username: "username",
		api_key:  "api_key",
		authurl:  "authurl",
	}
	err := c.Authenticate()
	if err != nil {
		panic(err)
	}
	fmt.Println(c)
	containers, err := c.ListContainers(0, "")
	fmt.Println(containers, err)
	containerinfos, err2 := c.ListContainersInfo(0, "")
	fmt.Println(containerinfos, err2)
}
