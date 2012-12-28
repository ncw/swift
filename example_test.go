// Copyright...

// This example demonstrates opening a Connection and doing some basic operations.
package swift_test

import (
	"fmt"
	"github.com/ncw/swift"
)

func Example() {
	// Create a connection
	c := swift.Connection{
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
	containers, err := c.ContainerNames(nil)
	fmt.Println(containers)
	// etc...
}

var container string

func ExampleConnection_ObjectsWalk() {
	objects := make([]string, 0)
	err := c.ObjectsWalk(container, nil, func(opts *swift.ObjectsOpts) (interface{}, error) {
		newObjects, err := c.ObjectNames(container, opts)
		if err == nil {
			objects = append(objects, newObjects...)
		}
		return newObjects, err
	})
	fmt.Println("Found all the objects", objects, err)
}
