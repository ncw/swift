package swift

import (
	"fmt"
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

)

// Auth defines the operations needed to authenticate with swift
//
// This encapsulates the different authentication schemes in use
type Authenticator interface {
	Request(*Connection) (*http.Request, error)
	Response(resp *http.Response) error
	// The public storage URL - set Internal to true to read
	// internal/service net URL
	StorageUrl(Internal bool) string
	// The access token
	Token() string
	// The CDN url if available
	CdnUrl() string
}

// newAuth - create a new Authenticator from the AuthUrl
//
// A hint for AuthVersion can be provided
func newAuth(c *Connection) (Authenticator, error) {
	AuthVersion := c.AuthVersion
	if AuthVersion == 0 {
		if strings.Contains(c.AuthUrl, "v3") {
			AuthVersion = 3
		} else if strings.Contains(c.AuthUrl, "v2") {
			AuthVersion = 2
		} else if strings.Contains(c.AuthUrl, "v1") {
			AuthVersion = 1
		} else {
			return nil, newErrorf(500, "Can't find AuthVersion in AuthUrl - set explicitly")
		}
	}
	switch AuthVersion {
	case 1:
		return &v1Auth{}, nil
	case 2:
		return &v2Auth{
			// Guess as to whether using API key or
			// password it will try both eventually so
			// this is just an optimization.
			useApiKey: len(c.ApiKey) >= 32,
		}, nil
	case 3:
		return &v3Auth {
		}, nil
	}
	return nil, newErrorf(500, "Auth Version %d not supported", AuthVersion)
}

// ------------------------------------------------------------

// v1 auth
type v1Auth struct {
	Headers http.Header // V1 auth: the authentication headers so extensions can access them
}

// v1 Authentication - make request
func (auth *v1Auth) Request(c *Connection) (*http.Request, error) {
	req, err := http.NewRequest("GET", c.AuthUrl, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set("X-Auth-Key", c.ApiKey)
	req.Header.Set("X-Auth-User", c.UserName)
	return req, nil
}

// v1 Authentication - read response
func (auth *v1Auth) Response(resp *http.Response) error {
	auth.Headers = resp.Header
	return nil
}

// v1 Authentication - read storage url
func (auth *v1Auth) StorageUrl(Internal bool) string {
	storageUrl := auth.Headers.Get("X-Storage-Url")
	if Internal {
		newUrl, err := url.Parse(storageUrl)
		if err != nil {
			return storageUrl
		}
		newUrl.Host = "snet-" + newUrl.Host
		storageUrl = newUrl.String()
	}
	return storageUrl
}

// v1 Authentication - read auth token
func (auth *v1Auth) Token() string {
	return auth.Headers.Get("X-Auth-Token")
}

// v1 Authentication - read cdn url
func (auth *v1Auth) CdnUrl() string {
	return auth.Headers.Get("X-CDN-Management-Url")
}

// ------------------------------------------------------------

// v2 Authentication
type v2Auth struct {
	Auth        *v2AuthResponse
	Region      string
	useApiKey   bool // if set will use API key not Password
	useApiKeyOk bool // if set won't change useApiKey any more
	notFirst    bool // set after first run
}

// v2 Authentication - make request
func (auth *v2Auth) Request(c *Connection) (*http.Request, error) {
	auth.Region = c.Region
	// Toggle useApiKey if not first run and not OK yet
	if auth.notFirst && !auth.useApiKeyOk {
		auth.useApiKey = !auth.useApiKey
	}
	auth.notFirst = true
	// Create a V2 auth request for the body of the connection
	var v2i interface{}
	if !auth.useApiKey {
		// Normal swift authentication
		v2 := v2AuthRequest{}
		v2.Auth.PasswordCredentials.UserName = c.UserName
		v2.Auth.PasswordCredentials.Password = c.ApiKey
		v2.Auth.Tenant = c.Tenant
		v2.Auth.TenantId = c.TenantId
		v2i = v2
	} else {
		// Rackspace special with API Key
		v2 := v2AuthRequestRackspace{}
		v2.Auth.ApiKeyCredentials.UserName = c.UserName
		v2.Auth.ApiKeyCredentials.ApiKey = c.ApiKey
		v2.Auth.Tenant = c.Tenant
		v2.Auth.TenantId = c.TenantId
		v2i = v2
	}
	body, err := json.Marshal(v2i)
	if err != nil {
		return nil, err
	}
	url := c.AuthUrl
	if !strings.HasSuffix(url, "/") {
		url += "/"
	}
	url += "tokens"
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

// v2 Authentication - read response
func (auth *v2Auth) Response(resp *http.Response) error {
	auth.Auth = new(v2AuthResponse)
	err := readJson(resp, auth.Auth)
	// If successfully read Auth then no need to toggle useApiKey any more
	if err == nil {
		auth.useApiKeyOk = true
	}
	return err
}

// Finds the Endpoint Url of "type" from the v2AuthResponse using the
// Region if set or defaulting to the first one if not
//
// Returns "" if not found
func (auth *v2Auth) endpointUrl(Type string, Internal bool) string {
	for _, catalog := range auth.Auth.Access.ServiceCatalog {
		if catalog.Type == Type {
			for _, endpoint := range catalog.Endpoints {
				if auth.Region == "" || (auth.Region == endpoint.Region) {
					if Internal {
						return endpoint.InternalUrl
					} else {
						return endpoint.PublicUrl
					}
				}
			}
		}
	}
	return ""
}

// v2 Authentication - read storage url
//
// If Internal is true then it reads the private (internal / service
// net) URL.
func (auth *v2Auth) StorageUrl(Internal bool) string {
	return auth.endpointUrl("object-store", Internal)
}

// v2 Authentication - read auth token
func (auth *v2Auth) Token() string {
	return auth.Auth.Access.Token.Id
}

// v2 Authentication - read cdn url
func (auth *v2Auth) CdnUrl() string {
	return auth.endpointUrl("rax:object-cdn", false)
}

// ------------------------------------------------------------

// V2 Authentication request
//
// http://docs.openstack.org/developer/keystone/api_curl_examples.html
// http://docs.rackspace.com/servers/api/v2/cs-gettingstarted/content/curl_auth.html
// http://docs.openstack.org/api/openstack-identity-service/2.0/content/POST_authenticate_v2.0_tokens_.html
type v2AuthRequest struct {
	Auth struct {
		PasswordCredentials struct {
			UserName string `json:"username"`
			Password string `json:"password"`
		} `json:"passwordCredentials"`
		Tenant   string `json:"tenantName,omitempty"`
		TenantId string `json:"tenantId,omitempty"`
	} `json:"auth"`
}

// V2 Authentication request - Rackspace variant
//
// http://docs.openstack.org/developer/keystone/api_curl_examples.html
// http://docs.rackspace.com/servers/api/v2/cs-gettingstarted/content/curl_auth.html
// http://docs.openstack.org/api/openstack-identity-service/2.0/content/POST_authenticate_v2.0_tokens_.html
type v2AuthRequestRackspace struct {
	Auth struct {
		ApiKeyCredentials struct {
			UserName string `json:"username"`
			ApiKey   string `json:"apiKey"`
		} `json:"RAX-KSKEY:apiKeyCredentials"`
		Tenant   string `json:"tenantName,omitempty"`
		TenantId string `json:"tenantId,omitempty"`
	} `json:"auth"`
}

// V2 Authentication reply
//
// http://docs.openstack.org/developer/keystone/api_curl_examples.html
// http://docs.rackspace.com/servers/api/v2/cs-gettingstarted/content/curl_auth.html
// http://docs.openstack.org/api/openstack-identity-service/2.0/content/POST_authenticate_v2.0_tokens_.html
type v2AuthResponse struct {
	Access struct {
		ServiceCatalog []struct {
			Endpoints []struct {
				InternalUrl string
				PublicUrl   string
				Region      string
				TenantId    string
			}
			Name string
			Type string
		}
		Token struct {
			Expires string
			Id      string
			Tenant  struct {
				Id   string
				Name string
			}
		}
		User struct {
			DefaultRegion string `json:"RAX-AUTH:defaultRegion"`
			Id            string
			Name          string
			Roles         []struct {
				Description string
				Id          string
				Name        string
				TenantId    string
			}
		}
	}
}

// ------------------------------------------------------------

// V3 Authentication request
//
// http://docs.openstack.org/developer/keystone/api_curl_examples.html
// http://developer.openstack.org/api-ref-identity-v3.html
type v3AuthRequest struct {
	Auth struct {
		Identity struct {
			Methods []string `json:"methods"`
			Password struct {
				User struct {
					Domain struct {
						Name string `json:"name,omitempty"`
						Id string `json:"id,omitempty"`
					} `json:"domain,omitempty"`
					Name string `json:"name"`
					Password string `json:"password"`
				} `json:"user"`
			} `json:"password"`
		} `json:"identity"`
		Scope *v3AuthRequestScope `json:"scope,omitempty"`
	}  `json:"auth"`
}

type v3AuthRequestScope struct {
	Project struct {
		Id string `json:"id,omitempty"`
	} `json:"project"`
}

// V3 Authentication response
type v3AuthResponse struct {
	Token struct {
		Expires_At string
		Issued_At string
		Methods []string
		Roles []map[string]string

		Project struct {
			Domain struct {
				Id string
				Name string
			}
			Id string
			Name string
		}

		Catalog []struct {
			Endpoints []struct {
				Region_Id string
				Url string
				Region string
				Interface string
				Id string
			}
			Type string
			Id string
			Name string
		}

		User struct {
			Domain struct {
				Id string
				Links struct {
					Self string
				}
				Name string
			}
			Id string
			Name string
		}

		Audit_Ids []string
	}
}

type v3Auth struct {
	Auth *v3AuthResponse
	Headers http.Header
}

func (auth *v3Auth) Request(c *Connection) (*http.Request, error){

	var v3i interface{}

	v3 := v3AuthRequest{}
	v3.Auth.Identity.Methods = []string{"password"}
	v3.Auth.Identity.Password.User.Name = c.UserName

	v3.Auth.Identity.Password.User.Password = c.ApiKey
	v3.Auth.Identity.Password.User.Domain.Name = c.Domain

	v3.Auth.Scope = new(v3AuthRequestScope)

	v3.Auth.Scope.Project.Id = c.TenantId


	v3i = v3
	body, err := json.Marshal(v3i)
	if err != nil {
		return nil, err
	}
	fmt.Printf("%s", body)
	url := c.AuthUrl
	if !strings.HasSuffix(url, "/") {
		url += "/"
	}
	url += "tokens"
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func (auth *v3Auth) Response(resp *http.Response) error{
	auth.Auth = new(v3AuthResponse)
	auth.Headers = resp.Header
	err := readJson(resp, auth.Auth)
	return err
}

func (auth *v3Auth) endpointUrl(Type string, Internal bool) string {
	for _, catalog := range auth.Auth.Token.Catalog {
		if catalog.Type == Type {
			for _, endpoint := range catalog.Endpoints {
				if Internal {
					if endpoint.Interface == "internal" {
						return endpoint.Url
					}
				} else {
					if endpoint.Interface == "public" {
						return endpoint.Url
					}
				}
			}
		}
	}
	return ""
}


func (auth *v3Auth) StorageUrl(Internal bool) string {
	return auth.endpointUrl("object-store", Internal)
}

func (auth *v3Auth) Token() string  {
	return auth.Headers.Get("X-Subject-Token")
}

func (auth *v3Auth) CdnUrl() string {
	return ""
}
