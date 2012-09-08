// Copyright...

// This example demonstrates opening a Connection and doing some basic operations.
package swift_test

import (
        "github.com/ncw/swift"
	"fmt"
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
	containers, err := c.ListContainers(nil)
	fmt.Println(containers)
	// etc...
}
