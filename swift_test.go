package swift_test

import (
	"fmt"
	"github.com/ncw/swift"
	"os"
	"testing"
)

var c swift.Connection
var m1 = swift.Metadata{"Hello": "1", "potato-Salad": "2"}
var m2 = swift.Metadata{"hello": "", "potato-salad": ""}

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
		t.Error(err)
	}
	fmt.Println("Authenticated")
	// FIXME look in c
}

func TestAccountInfo(t *testing.T) {
	fmt.Println(c.AccountInfo())
}
func TestUpdateAccount(t *testing.T) {
	fmt.Println(c.UpdateAccount(m1.AccountHeaders()))
	fmt.Println(c.AccountInfo())
	fmt.Println(c.UpdateAccount(m2.AccountHeaders()))
	fmt.Println(c.AccountInfo())
}
func TestListContainers(t *testing.T) {
	containers, err := c.ListContainers(nil)
	fmt.Println(containers, err)
}
func TestListContainersInfo(t *testing.T) {
	containerinfos, err2 := c.ListContainersInfo(nil)
	fmt.Println(containerinfos, err2)
}
func TestListObjects(t *testing.T) {
	objects, err3 := c.ListObjects("SquirrelSave", nil)
	fmt.Println(objects, err3)
}
func TestListObjectsInfo(t *testing.T) {
	objectsinfo, err4 := c.ListObjectsInfo("SquirrelSave", &swift.ListObjectsOpts{Delimiter: '/'})
	fmt.Println(objectsinfo, err4)
}
func TestListObjectsWithPath(t *testing.T) {
	objects, err3 := c.ListObjects("SquirrelSave", &swift.ListObjectsOpts{Delimiter: '/', Path: ""})
	fmt.Println(objects, err3)
	objects, err3 = c.ListObjects("SquirrelSave", &swift.ListObjectsOpts{Delimiter: '/', Path: "Downloads/"})
	fmt.Println(objects, err3)
}
func TestCreateContainer(t *testing.T) {
	fmt.Println(c.CreateContainer("sausage", m1.ContainerHeaders()))
}
func TestContainerInfo(t *testing.T) {
	fmt.Println(c.ContainerInfo("sausage"))
}
func TestUpdateContainer(t *testing.T) {
	fmt.Println(c.UpdateContainer("sausage", m2.ContainerHeaders()))
	fmt.Println(c.ContainerInfo("sausage"))
}
func TestCreateObjectString(t *testing.T) {
	fmt.Println("Create", c.CreateObjectString("sausage", "test_object", "12345", ""))
}
func TestGetObjectString(t *testing.T) {
	fmt.Println(c.GetObjectString("sausage", "test_object"))
}
func TestDeleteObject(t *testing.T) {
	fmt.Println("delete 1")
	fmt.Println(c.DeleteObject("sausage", "test_object"))
	fmt.Println("delete 1")
	fmt.Println(c.DeleteObject("sausage", "test_object"))
}
func TestDeleteContainer(t *testing.T) {
	fmt.Println("delete container 1")
	fmt.Println(c.DeleteContainer("sausage"))
	fmt.Println("delete container again")
	fmt.Println(c.DeleteContainer("sausage"))
	fmt.Println(c.ContainerInfo("SquirrelSave"))
}
