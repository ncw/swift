package swift

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
)

const (
	AUTH_METHOD_TOKEN         = "token"
  AUTH_METHOD_PASSWORD      = "password"
  INTERFACE_PUBLIC          = "public"
  INTERFACE_INTERNAL        = "internal"
  INTERFACE_ADMIN           = "admin"
  CATALOT_TYPE_OBJECT_STORE = "object-store"
)


// V3 Authentication request
// http://docs.openstack.org/developer/keystone/api_curl_examples.html
// http://developer.openstack.org/api-ref-identity-v3.html
type v3AuthRequest struct {
	Auth struct {
		Identity struct {
			Methods    []string        `json:"methods"`
			Password   *v3AuthPassword `json:"password,omitempty"`
      Token      *v3AuthToken    `json:"token,omitempty"`
		} `json:"identity"`
		Scope *v3Scope               `json:"scope,omitempty"`
	}  `json:"auth"`
}

type v3Scope struct {
	Project *v3Project   `json:"project,omitempty"`
  Domain  *v3Domain    `json:"domain,omitempty"`
}

type v3Domain struct {
  Id string   `json:"id,omitempty"`
  Name string `json:"name,omitempty"`
}

type v3Project struct {
  Name    string      `json:"name,omitempty"`
  Id      string      `json:"id,omitempty"`
  Domain  *v3Domain   `json:"domain,omitempty"`
}

type v3User struct {
  Domain *v3Domain  `json:"domain,omitempty"`
  Id string         `json:"id,omitempty"`
  Name string       `json:"name,omitempty"`
  Password string   `json:"password,omitempty"`
}

type v3AuthToken struct {
  Id string `json:"id"`
}

type v3AuthPassword struct {
  User v3User `json:"user"`
}

// V3 Authentication response
type v3AuthResponse struct {
	Token struct {
<<<<<<< HEAD
		Expires_At string
		Issued_At string
=======
		Expires_At, Issued_At string
>>>>>>> 703523b... add v3 support
		Methods []string
		Roles []map[string]string

		Project struct {
			Domain struct {
<<<<<<< HEAD
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
=======
				Id, Name string
			}
			Id, Name string
		}

		Catalog []struct {
      Id, Namem, Type string
			Endpoints []struct {
        Id, Region_Id, Url, Region, Interface string
			}
		}

		User struct {
      Id, Name string
			Domain struct {
				Id,	Name string
				Links struct {
					Self string
				}
			}
>>>>>>> 703523b... add v3 support
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

	if c.UserName == "" {
		v3.Auth.Identity.Methods = []string { AUTH_METHOD_TOKEN }
<<<<<<< HEAD
    v3.Auth.Identity.Token = new (v3AuthToken)
    v3.Auth.Identity.Token.Id = c.ApiKey
	} else {
    v3.Auth.Identity.Methods = []string { AUTH_METHOD_PASSWORD }
    v3.Auth.Identity.Password = new(v3AuthPassword)
    v3.Auth.Identity.Password.User = v3User {
      Name : c.UserName,
      Password : c.ApiKey,
    }
=======
    v3.Auth.Identity.Token = &v3AuthToken { Id: c.ApiKey }
	} else {
    v3.Auth.Identity.Methods = []string { AUTH_METHOD_PASSWORD }
    v3.Auth.Identity.Password = &v3AuthPassword {
      User: v3User {
        Name : c.UserName,
        Password : c.ApiKey,
      },
    }

>>>>>>> 703523b... add v3 support
    var domain *v3Domain

    if c.Domain != "" {
      domain = &v3Domain{ Name: c.Domain }
    } else if c.DomainId != "" {
      domain = &v3Domain{ Id: c.DomainId}
    }
    v3.Auth.Identity.Password.User.Domain = domain
	}

  if c.TenantId != "" || c.Tenant != "" {
<<<<<<< HEAD
  	v3.Auth.Scope = new(v3Scope)
    v3.Auth.Scope.Project = new(v3Project)
  	if c.TenantId != "" {
      v3.Auth.Scope.Project.Id = c.TenantId
    }
    if c.Tenant != "" {
      v3.Auth.Scope.Project.Name = c.Tenant
=======

  	v3.Auth.Scope = &v3Scope{ Project: &v3Project{} }

  	if c.TenantId != "" {
      v3.Auth.Scope.Project.Id = c.TenantId
    } else if c.Tenant != "" {
      v3.Auth.Scope.Project.Name = c.Tenant
      var defaultDomain v3Domain
      if c.Domain != "" {
        defaultDomain = v3Domain{ Name: "Default" }
      } else if c.DomainId != "" {
        defaultDomain = v3Domain{ Id: "Default" }
      }
      v3.Auth.Scope.Project.Domain = &defaultDomain
>>>>>>> 703523b... add v3 support
    }
  }

	v3i = v3
<<<<<<< HEAD
=======

>>>>>>> 703523b... add v3 support
	body, err := json.Marshal(v3i)
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

func (auth *v3Auth) Response(resp *http.Response) error{
<<<<<<< HEAD
	auth.Auth = new(v3AuthResponse)
=======
	auth.Auth = &v3AuthResponse{}
>>>>>>> 703523b... add v3 support
	auth.Headers = resp.Header
	err := readJson(resp, auth.Auth)
	return err
}

func (auth *v3Auth) endpointUrl(Type string, Internal bool) string {
	for _, catalog := range auth.Auth.Token.Catalog {
		if catalog.Type == Type {
			for _, endpoint := range catalog.Endpoints {
				if Internal {
					if endpoint.Interface == INTERFACE_INTERNAL {
						return endpoint.Url
					}
				} else {
					if endpoint.Interface == INTERFACE_PUBLIC {
						return endpoint.Url
					}
				}
			}
		}
	}
	return ""
}

func (auth *v3Auth) StorageUrl(Internal bool) string {
	return auth.endpointUrl(CATALOT_TYPE_OBJECT_STORE, Internal)
}

func (auth *v3Auth) Token() string  {
	return auth.Headers.Get("X-Subject-Token")
}

func (auth *v3Auth) CdnUrl() string {
	return ""
}
