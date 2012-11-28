// This tests the swift packagae
//
// It needs access to a real swift server which should be set up in
// the environment variables SWIFT_API_USER, SWIFT_API_KEY and
// SWIFT_AUTH_URL - see Testing in README.md for more info
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
	"time"
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

func TestAccount(t *testing.T) {
	info, headers, err := c.Account()
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

func TestAccountUpdate(t *testing.T) {
	err := c.AccountUpdate(m1.AccountHeaders())
	if err != nil {
		t.Fatal(err)
	}

	_, headers, err := c.Account()
	if err != nil {
		t.Fatal(err)
	}
	compareMaps(t, headers.AccountMetadata(), map[string]string{"hello": "1", "potato-salad": "2"})

	err = c.AccountUpdate(m2.AccountHeaders())
	if err != nil {
		t.Fatal(err)
	}

	_, headers, err = c.Account()
	if err != nil {
		t.Fatal(err)
	}
	compareMaps(t, headers.AccountMetadata(), map[string]string{})

	//fmt.Println(c.Account())
	//fmt.Println(headers)
	//fmt.Println(headers.AccountMetadata())
	//fmt.Println(c.AccountUpdate(m2.AccountHeaders()))
	//fmt.Println(c.Account())
}

func TestContainerCreate(t *testing.T) {
	err := c.ContainerCreate(CONTAINER, m1.ContainerHeaders())
	if err != nil {
		t.Fatal(err)
	}
}

func TestContainer(t *testing.T) {
	info, headers, err := c.Container(CONTAINER)
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

func TestContainersAll(t *testing.T) {
	containers1, err := c.ContainersAll(nil)
	if err != nil {
		t.Fatal(err)
	}
	containers2, err := c.Containers(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(containers1) != len(containers2) {
		t.Fatal("Wrong length")
	}
	for i := range containers1 {
		if containers1[i] != containers2[i] {
			t.Fatal("Not the same")
		}
	}
}

func TestContainersAllWithLimit(t *testing.T) {
	containers1, err := c.ContainersAll(&swift.ContainersOpts{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	containers2, err := c.Containers(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(containers1) != len(containers2) {
		t.Fatal("Wrong length")
	}
	for i := range containers1 {
		if containers1[i] != containers2[i] {
			t.Fatal("Not the same")
		}
	}
}

func TestContainerUpdate(t *testing.T) {
	err := c.ContainerUpdate(CONTAINER, m2.ContainerHeaders())
	if err != nil {
		t.Fatal(err)
	}
	_, headers, err := c.Container(CONTAINER)
	if err != nil {
		t.Fatal(err)
	}
	compareMaps(t, headers.ContainerMetadata(), map[string]string{})
	//fmt.Println(headers)
}

func TestContainerNames(t *testing.T) {
	containers, err := c.ContainerNames(nil)
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

func TestContainerNamesAll(t *testing.T) {
	containers1, err := c.ContainerNamesAll(nil)
	if err != nil {
		t.Fatal(err)
	}
	containers2, err := c.ContainerNames(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(containers1) != len(containers2) {
		t.Fatal("Wrong length")
	}
	for i := range containers1 {
		if containers1[i] != containers2[i] {
			t.Fatal("Not the same")
		}
	}
}

func TestContainerNamesAllWithLimit(t *testing.T) {
	containers1, err := c.ContainerNamesAll(&swift.ContainersOpts{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	containers2, err := c.ContainerNames(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(containers1) != len(containers2) {
		t.Fatal("Wrong length")
	}
	for i := range containers1 {
		if containers1[i] != containers2[i] {
			t.Fatal("Not the same")
		}
	}
}

func TestObjectPutString(t *testing.T) {
	err := c.ObjectPutString(CONTAINER, OBJECT, CONTENTS, "")
	if err != nil {
		t.Fatal(err)
	}
}

func TestObjectGetString(t *testing.T) {
	contents, err := c.ObjectGetString(CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	if contents != CONTENTS {
		t.Error("Contents wrong")
	}
	//fmt.Println(contents)
}

func TestObjectUpdate(t *testing.T) {
	err := c.ObjectUpdate(CONTAINER, OBJECT, m1.ObjectHeaders())
	if err != nil {
		t.Fatal(err)
	}
}

func checkTime(t *testing.T, when time.Time, low, high int) {
	dt := time.Now().Sub(when)
	if dt < time.Duration(low)*time.Second || dt > time.Duration(high)*time.Second {
		t.Errorf("Time is wrong: dt=%q, when=%q", dt, when)
	}
}

func TestObject(t *testing.T) {
	object, headers, err := c.Object(CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	compareMaps(t, headers.ObjectMetadata(), map[string]string{"hello": "1", "potato-salad": "2"})
	if object.Name != OBJECT || object.Bytes != CONTENT_SIZE || object.ContentType != "application/octet-stream" || object.Hash != CONTENT_MD5 || object.PseudoDirectory != false || object.SubDir != "" {
		t.Error("Bad object info %q", object)
	}
	checkTime(t, object.LastModified, -10, 10)
}

func TestObjectUpdate2(t *testing.T) {
	err := c.ObjectUpdate(CONTAINER, OBJECT, m2.ObjectHeaders())
	if err != nil {
		t.Fatal(err)
	}
	_, headers, err := c.Object(CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	//fmt.Println(headers, headers.ObjectMetadata())
	compareMaps(t, headers.ObjectMetadata(), map[string]string{"hello": "", "potato-salad": ""})
}

// FIXME more tests for ObjectPut and ObjectGet

func TestContainers(t *testing.T) {
	containers, err := c.Containers(nil)
	if err != nil {
		t.Fatal(err)
	}
	ok := false
	for _, container := range containers {
		if container.Name == CONTAINER {
			ok = true
			// Container may or may not have the file contents in it
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

func TestObjectNames(t *testing.T) {
	objects, err := c.ObjectNames(CONTAINER, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0] != OBJECT {
		t.Error("Incorrect listing", objects)
	}
	//fmt.Println(objects)
}

func TestObjectNamesAll(t *testing.T) {
	objects, err := c.ObjectNamesAll(CONTAINER, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0] != OBJECT {
		t.Error("Incorrect listing", objects)
	}
	//fmt.Println(objects)
}

func TestObjectNamesAllWithLimit(t *testing.T) {
	objects, err := c.ObjectNamesAll(CONTAINER, &swift.ObjectsOpts{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0] != OBJECT {
		t.Error("Incorrect listing", objects)
	}
	//fmt.Println(objects)
}

func TestObjects(t *testing.T) {
	objects, err := c.Objects(CONTAINER, &swift.ObjectsOpts{Delimiter: '/'})
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
	checkTime(t, object.LastModified, -10, 10)
	// fmt.Println(objects)
}

func TestObjectsAll(t *testing.T) {
	objects, err := c.ObjectsAll(CONTAINER, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0].Name != OBJECT {
		t.Error("Incorrect listing", objects)
	}
	//fmt.Println(objects)
}

func TestObjectsAllWithLimit(t *testing.T) {
	objects, err := c.ObjectsAll(CONTAINER, &swift.ObjectsOpts{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0].Name != OBJECT {
		t.Error("Incorrect listing", objects)
	}
	//fmt.Println(objects)
}

func TestObjectNamesWithPath(t *testing.T) {
	objects, err := c.ObjectNames(CONTAINER, &swift.ObjectsOpts{Delimiter: '/', Path: ""})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0] != OBJECT {
		t.Error("Bad listing with path", objects)
	}
	// fmt.Println(objects)
	objects, err = c.ObjectNames(CONTAINER, &swift.ObjectsOpts{Delimiter: '/', Path: "Downloads/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 0 {
		t.Error("Bad listing with path", objects)
	}
	// fmt.Println(objects)
}

func TestObjectDelete(t *testing.T) {
	err := c.ObjectDelete(CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	err = c.ObjectDelete(CONTAINER, OBJECT)
	if err != swift.ObjectNotFound {
		t.Fatal("Expecting Object not found", err)
	}
}

func TestContainerDelete(t *testing.T) {
	err := c.ContainerDelete(CONTAINER)
	if err != nil {
		t.Fatal(err)
	}
	err = c.ContainerDelete(CONTAINER)
	if err != swift.ContainerNotFound {
		t.Fatal("Expecting container not found", err)
	}
	_, _, err = c.Container(CONTAINER)
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
