// Copyright...

// This example demonstrates opening a Connection and doing some basic operations.
package swift_test

import (
	"fmt"
	"github.com/ncw/swift"
)

func Example() {
	// Create a connection
	c = swift.Connection{
		UserName: "user",
		ApiKey:   "key",
		AuthUrl:  "auth_url",
	}
	// Authenticate
	err := c.Authenticate()
	if err != nil {
		panic(err)
	}
	// List all the containers
	containers, headers, err := c.ListContainers(nil, nil)
	fmt.Println(containers, headers)
	// etc...
}
