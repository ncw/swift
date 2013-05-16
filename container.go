package swift

import (
	"net/http"
	"net/url"
	"strconv"
)

// ContainersOpts is options for Containers() and ContainerNames()
type ContainersOpts struct {
	Limit     int     // For an integer value n, limits the number of results to at most n values.
	Marker    string  // Given a string value x, return object names greater in value than the specified marker.
	EndMarker string  // Given a string value x, return container names less in value than the specified marker.
	Headers   Headers // Any additional HTTP headers - can be nil
}

// parse the ContainerOpts
func (opts *ContainersOpts) parse() (url.Values, Headers) {
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
		h = opts.Headers
	}
	return v, h
}

// ContainerNames returns a slice of names of containers in this account.
func (c *Connection) ContainerNames(opts *ContainersOpts) ([]string, error) {
	v, h := opts.parse()
	resp, _, err := c.storage(RequestOpts{
		Operation:  "GET",
		Parameters: v,
		ErrorMap:   ContainerErrorMap,
		Headers:    h,
	})
	if err != nil {
		return nil, err
	}
	lines, err := readLines(resp)
	return lines, err
}

// Container contains information about a container
type Container struct {
	Name  string // Name of the container
	Count int64  // Number of objects in the container
	Bytes int64  // Total number of bytes used in the container
}

// Containers returns a slice of structures with full information as
// described in Container.
func (c *Connection) Containers(opts *ContainersOpts) ([]Container, error) {
	v, h := opts.parse()
	v.Set("format", "json")
	resp, _, err := c.storage(RequestOpts{
		Operation:  "GET",
		Parameters: v,
		ErrorMap:   ContainerErrorMap,
		Headers:    h,
	})
	if err != nil {
		return nil, err
	}
	var containers []Container
	err = readJson(resp, &containers)
	return containers, err
}

// containersAllOpts makes a copy of opts if set or makes a new one and
// overrides Limit and Marker
func containersAllOpts(opts *ContainersOpts) *ContainersOpts {
	var newOpts ContainersOpts
	if opts != nil {
		newOpts = *opts
	}
	if newOpts.Limit == 0 {
		newOpts.Limit = allContainersLimit
	}
	newOpts.Marker = ""
	return &newOpts
}

// ContainersAll is like Containers but it returns all the Containers
//
// It calls Containers multiple times using the Marker parameter
//
// It has a default Limit parameter but you may pass in your own
func (c *Connection) ContainersAll(opts *ContainersOpts) ([]Container, error) {
	opts = containersAllOpts(opts)
	containers := make([]Container, 0)
	for {
		newContainers, err := c.Containers(opts)
		if err != nil {
			return nil, err
		}
		containers = append(containers, newContainers...)
		if len(newContainers) < opts.Limit {
			break
		}
		opts.Marker = newContainers[len(newContainers)-1].Name
	}
	return containers, nil
}

// ContainerNamesAll is like ContainerNamess but it returns all the Containers
//
// It calls ContainerNames multiple times using the Marker parameter
//
// It has a default Limit parameter but you may pass in your own
func (c *Connection) ContainerNamesAll(opts *ContainersOpts) ([]string, error) {
	opts = containersAllOpts(opts)
	containers := make([]string, 0)
	for {
		newContainers, err := c.ContainerNames(opts)
		if err != nil {
			return nil, err
		}
		containers = append(containers, newContainers...)
		if len(newContainers) < opts.Limit {
			break
		}
		opts.Marker = newContainers[len(newContainers)-1]
	}
	return containers, nil
}

/* ------------------------------------------------------------ */

// ContainerCreate creates a container.
//
// If you don't want to add Headers just pass in nil
//
// No error is returned if it already exists but the metadata if any will be updated.
func (c *Connection) ContainerCreate(container string, h Headers) error {
	_, _, err := c.storage(RequestOpts{
		Container:  container,
		Operation:  "PUT",
		ErrorMap:   ContainerErrorMap,
		NoResponse: true,
		Headers:    h,
	})
	return err
}

// ContainerDelete deletes a container.
//
// May return ContainerDoesNotExist or ContainerNotEmpty
func (c *Connection) ContainerDelete(container string) error {
	_, _, err := c.storage(RequestOpts{
		Container:  container,
		Operation:  "DELETE",
		ErrorMap:   ContainerErrorMap,
		NoResponse: true,
	})
	return err
}

// Container returns info about a single container including any
// metadata in the headers.
func (c *Connection) Container(container string) (info Container, headers Headers, err error) {
	var resp *http.Response
	resp, headers, err = c.storage(RequestOpts{
		Container:  container,
		Operation:  "HEAD",
		ErrorMap:   ContainerErrorMap,
		NoResponse: true,
	})
	if err != nil {
		return
	}
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

// ContainerUpdate adds, replaces or removes container metadata.
//
// Add or update keys by mentioning them in the Metadata.
//
// Remove keys by setting them to an empty string.
//
// Container metadata can only be read with Container() not with Containers().
func (c *Connection) ContainerUpdate(container string, h Headers) error {
	_, _, err := c.storage(RequestOpts{
		Container:  container,
		Operation:  "POST",
		ErrorMap:   ContainerErrorMap,
		NoResponse: true,
		Headers:    h,
	})
	return err
}
