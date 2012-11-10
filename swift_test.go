// This tests the swift packagae
//
// It needs access to a real swift server which should be set up in
// the environment variables SWIFT_API_USER, SWIFT_API_KEY and
// SWIFT_AUTH_URL - see README-test for more info
//
// The functions are designed to run in order and create things the
// next function tests.  This means that if it goes wrong it is likely
// errors will propagate.  You may need to tidy up the CONTAINER to
// get it to run cleanly.
package swift_test

import (
	"fmt"
	"github.com/ncw/swift"
	"os"
	"testing"
)

var (
	c  swift.Connection
	m1 = swift.Metadata{"Hello": "1", "potato-Salad": "2"}
	m2 = swift.Metadata{"hello": "", "potato-salad": ""}
)

const (
	CONTAINER    = "GoSwiftUnitTest"
	OBJECT       = "test_object"
	CONTENTS     = "12345"
	CONTENT_SIZE = int64(len(CONTENTS))
	CONTENT_MD5  = "827ccb0eea8a706c4c34a16891f84e7b"
)

// Test functions are run in order - this one must be first!
func TestAuthenticate(t *testing.T) {
	UserName := os.Getenv("SWIFT_API_USER")
	ApiKey := os.Getenv("SWIFT_API_KEY")
	AuthUrl := os.Getenv("SWIFT_AUTH_URL")
	if UserName == "" || ApiKey == "" || AuthUrl == "" {
		t.Fatal("SWIFT_API_USER, SWIFT_API_KEY and SWIFT_AUTH_URL not all set")
	}
	c = swift.Connection{
		UserName: UserName,
		ApiKey:   ApiKey,
		AuthUrl:  AuthUrl,
	}
	err := c.Authenticate()
	if err != nil {
		t.Fatal("Auth failed", err)
	}
	if !c.Authenticated() {
		t.Fatal("Not authenticated")
	}
}

func TestAccountInfo(t *testing.T) {
	info, headers, err := c.AccountInfo()
	if err != nil {
		t.Fatal(err)
	}
	if headers["X-Account-Container-Count"] != fmt.Sprintf("%d", info.Containers) {
		t.Error("Bad container count")
	}
	if headers["X-Account-Bytes-Used"] != fmt.Sprintf("%d", info.BytesUsed) {
		t.Error("Bad bytes count")
	}
	if headers["X-Account-Object-Count"] != fmt.Sprintf("%d", info.Objects) {
		t.Error("Bad objects count")
	}
	//fmt.Println(info)
	//fmt.Println(headers)
}

func compareMaps(t *testing.T, a, b map[string]string) {
	if len(a) != len(b) {
		t.Error("Maps different sizes", a, b)
	}
	for ka, va := range a {
		if va != b[ka] {
			t.Error("Difference in key", ka, va, b[ka])
		}
	}
	for kb, vb := range b {
		if vb != a[kb] {
			t.Error("Difference in key", kb, vb, a[kb])
		}
	}
}

func TestUpdateAccount(t *testing.T) {
	err := c.UpdateAccount(m1.AccountHeaders())
	if err != nil {
		t.Fatal(err)
	}

	_, headers, err := c.AccountInfo()
	if err != nil {
		t.Fatal(err)
	}
	compareMaps(t, headers.AccountMetadata(), map[string]string{"hello": "1", "potato-salad": "2"})

	err = c.UpdateAccount(m2.AccountHeaders())
	if err != nil {
		t.Fatal(err)
	}

	_, headers, err = c.AccountInfo()
	if err != nil {
		t.Fatal(err)
	}
	compareMaps(t, headers.AccountMetadata(), map[string]string{})

	//fmt.Println(c.AccountInfo())
	//fmt.Println(headers)
	//fmt.Println(headers.AccountMetadata())
	//fmt.Println(c.UpdateAccount(m2.AccountHeaders()))
	//fmt.Println(c.AccountInfo())
}

func TestCreateContainer(t *testing.T) {
	err := c.CreateContainer(CONTAINER, m1.ContainerHeaders())
	if err != nil {
		t.Fatal(err)
	}
}

func TestContainerInfo(t *testing.T) {
	info, headers, err := c.ContainerInfo(CONTAINER)
	if err != nil {
		t.Fatal(err)
	}
	compareMaps(t, headers.ContainerMetadata(), map[string]string{"hello": "1", "potato-salad": "2"})
	if CONTAINER != info.Name {
		t.Error("Bad container count")
	}
	if headers["X-Container-Bytes-Used"] != fmt.Sprintf("%d", info.Bytes) {
		t.Error("Bad bytes count")
	}
	if headers["X-Container-Object-Count"] != fmt.Sprintf("%d", info.Count) {
		t.Error("Bad objects count")
	}
	//fmt.Println(info)
	//fmt.Println(headers)
}

func TestUpdateContainer(t *testing.T) {
	err := c.UpdateContainer(CONTAINER, m2.ContainerHeaders())
	if err != nil {
		t.Fatal(err)
	}
	_, headers, err := c.ContainerInfo(CONTAINER)
	if err != nil {
		t.Fatal(err)
	}
	compareMaps(t, headers.ContainerMetadata(), map[string]string{})
	//fmt.Println(headers)
}

func TestListContainers(t *testing.T) {
	containers, err := c.ListContainers(nil)
	if err != nil {
		t.Fatal(err)
	}
	// fmt.Printf("container %q\n", CONTAINER)
	ok := false
	for _, container := range containers {
		if container == CONTAINER {
			ok = true
			break
		}
	}
	if !ok {
		t.Errorf("Didn't find container %q in listing %q", CONTAINER, containers)
	}
	// fmt.Println(containers)
}

func TestCreateObjectString(t *testing.T) {
	err := c.CreateObjectString(CONTAINER, OBJECT, CONTENTS, "")
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetObjectString(t *testing.T) {
	contents, err := c.GetObjectString(CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	if contents != CONTENTS {
		t.Error("Contents wrong")
	}
	//fmt.Println(contents)
}

func TestListContainersInfo(t *testing.T) {
	containers, err := c.ListContainersInfo(nil)
	if err != nil {
		t.Fatal(err)
	}
	ok := false
	for _, container := range containers {
		if container.Name == CONTAINER {
			ok = true
			// ContainerInfo may or may not have the file contents in it
			// Swift updates may be behind
			if container.Count == 0 && container.Bytes == 0 {
				break
			}
			if container.Count == 1 && container.Bytes == CONTENT_SIZE {
				break
			}
			t.Errorf("Bad size of Container %q: %q", CONTAINER, container)
			break
		}
	}
	if !ok {
		t.Errorf("Didn't find container %q in listing %q", CONTAINER, containers)
	}
	//fmt.Println(containers)
}

func TestListObjects(t *testing.T) {
	objects, err := c.ListObjects(CONTAINER, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0] != OBJECT {
		t.Error("Incorrect listing", objects)
	}
	//fmt.Println(objects)
}

func TestListObjectsInfo(t *testing.T) {
	objects, err := c.ListObjectsInfo(CONTAINER, &swift.ListObjectsOpts{Delimiter: '/'})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Fatal("Should only be 1 object")
	}
	object := objects[0]
	if object.Name != OBJECT || object.Bytes != CONTENT_SIZE || object.ContentType != "application/octet-stream" || object.Hash != CONTENT_MD5 || object.PseudoDirectory != false || object.SubDir != "" {
		t.Error("Bad object info %q", object)
	}
	// FIXME check object.LastModified
	// fmt.Println(objects)
}

func TestListObjectsWithPath(t *testing.T) {
	objects, err := c.ListObjects(CONTAINER, &swift.ListObjectsOpts{Delimiter: '/', Path: ""})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0] != OBJECT {
		t.Error("Bad listing with path", objects)
	}
	// fmt.Println(objects)
	objects, err = c.ListObjects(CONTAINER, &swift.ListObjectsOpts{Delimiter: '/', Path: "Downloads/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 0 {
		t.Error("Bad listing with path", objects)
	}
	// fmt.Println(objects)
}

func TestDeleteObject(t *testing.T) {
	err := c.DeleteObject(CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	err = c.DeleteObject(CONTAINER, OBJECT)
	if err != swift.ObjectNotFound {
		t.Fatal("Expecting Object not found", err)
	}
}

func TestDeleteContainer(t *testing.T) {
	err := c.DeleteContainer(CONTAINER)
	if err != nil {
		t.Fatal(err)
	}
	err = c.DeleteContainer(CONTAINER)
	if err != swift.ContainerNotFound {
		t.Fatal("Expecting container not found", err)
	}
	_, _, err = c.ContainerInfo(CONTAINER)
	if err != swift.ContainerNotFound {
		t.Fatal("Expecting container not found", err)
	}
}

func TestUnAuthenticate(t *testing.T) {
	c.UnAuthenticate()
	if c.Authenticated() {
		t.Fatal("Shouldn't be authenticated")
	}
	// Test re-authenticate
	err := c.Authenticate()
	if err != nil {
		t.Fatal("ReAuth failed", err)
	}
	if !c.Authenticated() {
		t.Fatal("Not authenticated")
	}
}
