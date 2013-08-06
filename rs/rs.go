package rs

import (
	"errors"
	"github.com/ncw/swift"
	"net/http"
	"strconv"
	"strings"
)

// RsConnection is a RackSpace specific wrapper to the core swift library which
// exposes the RackSpace CDN commands via the CDN Management URL interface.
// It can also take advantage of the faster internal "ServiceNet" network.
type RsConnection struct {
	swift.Connection
}

func (c *RsConnection) Authenticate() error {
	err := c.Connection.Authenticate()
	if err != nil {
		return err
	}

	c.StorageUrl = strings.Replace(c.StorageUrl, "https://", "https://snet-", 1)
	return nil
}

// manage is similar to the swift storage method, but uses the CDN Management URL for CDN specific calls.
func (c *RsConnection) manage(p swift.RequestOpts) (resp *http.Response, headers swift.Headers, err error) {
	url := c.AuthHeaders.Get("X-CDN-Management-Url")
	if url == "" {
		return nil, nil, errors.New("The X-CDN-Management-Url does not exist on the authenticated platform")
	}
	return c.Connection.Call(url, p)
}

// ContainerCDNEnable enables a container for public CDN usage.
//
// Change the default TTL of 259200 seconds (72 hours) by passing in an integer value.
//
// This method can be called again to change the TTL.
func (c *RsConnection) ContainerCDNEnable(container string, ttl int) (swift.Headers, error) {
	h := swift.Headers{"X-CDN-Enabled": "true"}
	if ttl > 0 {
		h["X-TTL"] = strconv.Itoa(ttl)
	}

	_, headers, err := c.manage(swift.RequestOpts{
		Container:  container,
		Operation:  "PUT",
		ErrorMap:   swift.ContainerErrorMap,
		NoResponse: true,
		Headers:    h,
	})
	return headers, err
}

// ContainerCDNDisable disables CDN access to a container.
func (c *RsConnection) ContainerCDNDisable(container string) error {
	h := swift.Headers{"X-CDN-Enabled": "false"}

	_, _, err := c.manage(swift.RequestOpts{
		Container:  container,
		Operation:  "PUT",
		ErrorMap:   swift.ContainerErrorMap,
		NoResponse: true,
		Headers:    h,
	})
	return err
}

// ContainerCDNMeta returns the CDN metadata for a container.
func (c *RsConnection) ContainerCDNMeta(container string) (swift.Headers, error) {
	_, headers, err := c.manage(swift.RequestOpts{
		Container:  container,
		Operation:  "HEAD",
		ErrorMap:   swift.ContainerErrorMap,
		NoResponse: true,
		Headers:    swift.Headers{},
	})
	return headers, err
}
