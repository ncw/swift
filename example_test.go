// Copyright...

// This example demonstrates opening a Connection and doing some basic operations.
package swift_test

import (
	"context"
	"fmt"

	"github.com/ncw/swift/v2"
)

func ExampleConnection() {
	ctx := context.Background()
	// Create a v1 auth connection
	c := &swift.Connection{
		// This should be your username
		UserName: "user",
		// This should be your api key
		ApiKey: "key",
		// This should be a v1 auth url, eg
		//  Rackspace US        https://auth.api.rackspacecloud.com/v1.0
		//  Rackspace UK        https://lon.auth.api.rackspacecloud.com/v1.0
		//  Memset Memstore UK  https://auth.storage.memset.com/v1.0
		AuthUrl: "auth_url",
	}

	// Authenticate
	err := c.Authenticate(ctx)
	if err != nil {
		panic(err)
	}
	// List all the containers
	containers, err := c.ContainerNames(ctx, nil)
	fmt.Println(containers)
	// etc...

	// ------ or alternatively create a v2 connection ------

	// Create a v2 auth connection
	c = &swift.Connection{
		// This is the sub user for the storage - eg "admin"
		UserName: "user",
		// This should be your api key
		ApiKey: "key",
		// This should be a version2 auth url, eg
		//  Rackspace v2        https://identity.api.rackspacecloud.com/v2.0
		//  Memset Memstore v2  https://auth.storage.memset.com/v2.0
		AuthUrl: "v2_auth_url",
		// Region to use - default is use first region if unset
		Region: "LON",
		// Name of the tenant - this is likely your username
		Tenant: "jim",
	}

	// as above...
}

var container string

func ExampleConnection_ObjectsWalk() {
	c, rollback := makeConnection(nil)
	defer rollback()

	objects := make([]string, 0)
	err := c.ObjectsWalk(context.Background(), container, nil, func(ctx context.Context, opts *swift.ObjectsOpts) (interface{}, error) {
		newObjects, err := c.ObjectNames(ctx, container, opts)
		if err == nil {
			objects = append(objects, newObjects...)
		}
		return newObjects, err
	})
	fmt.Println("Found all the objects", objects, err)
}

func ExampleConnection_VersionContainerCreate() {
	c, rollback := makeConnection(nil)
	defer rollback()

	// Use the helper method to create the current and versions container.
	if err := c.VersionContainerCreate(context.Background(), "cds", "cd-versions"); err != nil {
		fmt.Print(err.Error())
	}
}

func ExampleConnection_VersionEnable() {
	ctx := context.Background()
	c, rollback := makeConnection(nil)
	defer rollback()

	// Build the containers manually and enable them.
	if err := c.ContainerCreate(ctx, "movie-versions", nil); err != nil {
		fmt.Print(err.Error())
	}
	if err := c.ContainerCreate(ctx, "movies", nil); err != nil {
		fmt.Print(err.Error())
	}
	if err := c.VersionEnable(ctx, "movies", "movie-versions"); err != nil {
		fmt.Print(err.Error())
	}

	// Access the primary container as usual with ObjectCreate(), ObjectPut(), etc.
	// etc...
}

func ExampleConnection_VersionDisable() {
	c, rollback := makeConnection(nil)
	defer rollback()

	// Disable versioning on a container.  Note that this does not delete the versioning container.
	err := c.VersionDisable(context.Background(), "movies")
	if err != nil {
		panic(err)
	}
}
