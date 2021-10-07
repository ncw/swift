package swift

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/ncw/swift/v2/swifttest"
)

var srv *swifttest.SwiftServer
var con *Connection
var err error
var filecontent = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
var segmentContainer = "segment_container112"

func initTest(t *testing.T) {
	con, err = initTestConnection(t)
	if err != nil {
		t.Fail()
	}
}

func teardown() {
	if srv != nil {
		srv.Close()
	}
}

func initTestConnection(t *testing.T) (*Connection, error) {
	//Uses /swifttest
	//in-memory implementation to start
	//a swift object store to test against
	srv, err = swifttest.NewSwiftServer("localhost")
	if err != nil {
		return nil, err
	}
	swiftCon := Connection{
		UserName:       "swifttest",
		ApiKey:         "swifttest",
		AuthUrl:        srv.AuthURL,
		Region:         "",
		Tenant:         "",
		ConnectTimeout: time.Second,
		Timeout:        time.Second,
		Transport:      new(http.Transport),
		Domain:         "Default",
		AuthVersion:    1,
	}
	err = swiftCon.Authenticate(context.Background())
	return &swiftCon, err
}
func TestCases(t *testing.T) {
	initTest(t)
	createContainers([]string{"c1", "c2", segmentContainer}, t)
	createDynamicObject("c1", "o", t)
	moveDynamicObject("c1", "o", "c2", "oo", t)
	deleteDynamicObject("c2", "oo", t)
	createStaticObject("c2", "o2", t)
	moveStaticObject("c2", "o2", "c1", "oo2", t)
	deleteStaticObject("c1", "oo2", t)
	teardown()
}

func createContainers(containers []string, t *testing.T) {
	ctx := context.Background()
	for i := 0; i < len(containers); i++ {
		err = con.ContainerCreate(ctx, containers[i], nil) // Create container
		if err != nil {
			t.Errorf("Fail at create container %s", containers[i])
		}
	}
}

func createDynamicObject(container, object string, t *testing.T) {
	ctx := context.Background()
	metadata := map[string]string{}
	metadata["Custom-Field"] = "SomeValue"
	ops := LargeObjectOpts{
		Container:        container,                          // Name of container to place object
		ObjectName:       object,                             // Name of object
		CheckHash:        false,                              // If set Check the hash
		ContentType:      "application/octet-stream",         // Content-Type of the object
		Headers:          Metadata(metadata).ObjectHeaders(), // Additional headers to upload the object with
		SegmentContainer: segmentContainer,                   // Name of the container to place segments
		SegmentPrefix:    "sg",                               // Prefix to use for the segments
	}
	bigfile, err := con.DynamicLargeObjectCreate(ctx, &ops)
	if err != nil {
		t.Errorf("Fail at dynamic create Large Object")
	}
	_, err = bigfile.WriteWithContext(ctx, filecontent)
	if err != nil {
		t.Errorf("WriteWithContext failed: %v", err)
	}
	err = bigfile.CloseWithContext(ctx)
	if err != nil {
		t.Errorf("CloseWithContext failed: %v", err)
	}
	checkObject(container, object, t)
}
func checkObject(container, object string, t *testing.T) {
	ctx := context.Background()
	info, header, err := con.Object(ctx, container, object)
	if err != nil {
		t.Errorf("Fail at get Large Object metadata: %s", err.Error())
	}
	if !header.IsLargeObject() {
		t.Errorf("Fail: object is not a large object")
	}
	if info.Bytes != 10 {
		t.Errorf("Fail: mismatch content lengh")
	}
	if val, ok := header["X-Object-Meta-Custom-Field"]; !ok || val != "SomeValue" {
		t.Errorf("Fail: lost custom metadata header")
	}

	content, err := con.ObjectGetBytes(ctx, container, object)
	if err != nil {
		t.Errorf("Fail at read Large Object : %s", err.Error())
	}
	if !bytes.Equal(content, filecontent) {
		t.Errorf("Fail: mismatch content")
	}

}
func checkNotExistObject(container, object string, t *testing.T) {
	_, _, err = con.Object(context.Background(), container, object)
	if err == nil || err.Error() != "Object Not Found" {
		t.Errorf("Fail at checkNotExistObject object: %s", err)
	}
}
func moveDynamicObject(sc, so, dc, do string, t *testing.T) {
	err = con.DynamicLargeObjectMove(context.Background(), sc, so, dc, do)
	if err != nil {
		t.Errorf("Fail at dynamic move Large Object: %s", err.Error())
	}
	checkNotExistObject(sc, so, t)
	checkObject(dc, do, t)
}
func deleteDynamicObject(container, object string, t *testing.T) {
	ctx := context.Background()
	err = con.DynamicLargeObjectDelete(ctx, container, object)
	if err != nil {
		t.Errorf("Fail at delte dynamic Large Object: %s", err.Error())
	}
	checkNotExistObject(container, object, t)
	objs, err := con.ObjectsAll(ctx, segmentContainer, nil)
	if err != nil {
		t.Errorf("Fail at check delte dynamic Large Object: %s", err.Error())
	}
	if len(objs) != 0 {
		t.Errorf("Fail at check delte dynamic Large Object: seg not deleted")
	}
}
func createStaticObject(container, object string, t *testing.T) {
	ctx := context.Background()
	metadata := map[string]string{}
	metadata["Custom-Field"] = "SomeValue"
	ops := LargeObjectOpts{
		Container:        container,                          // Name of container to place object
		ObjectName:       object,                             // Name of object
		CheckHash:        false,                              // If set Check the hash
		ContentType:      "application/octet-stream",         // Content-Type of the object
		Headers:          Metadata(metadata).ObjectHeaders(), // Additional headers to upload the object with
		SegmentContainer: segmentContainer,                   // Name of the container to place segments
		SegmentPrefix:    "sg",                               // Prefix to use for the segments
	}
	bigfile, err := con.StaticLargeObjectCreate(ctx, &ops)
	if err != nil {
		t.Errorf("Fail at static create Large Object")
	}
	_, err = bigfile.WriteWithContext(ctx, filecontent)
	if err != nil {
		t.Errorf("WriteWithContext failed: %v", err)
	}
	err = bigfile.CloseWithContext(ctx)
	if err != nil {
		t.Errorf("CloseWithContext failed: %v", err)
	}
	checkObject(container, object, t)
}
func moveStaticObject(sc, so, dc, do string, t *testing.T) {
	err = con.StaticLargeObjectMove(context.Background(), sc, so, dc, do)
	if err != nil {
		t.Errorf("Fail at static move Large Object: %s", err.Error())
	}
	checkNotExistObject(sc, so, t)
	checkObject(dc, do, t)
}
func deleteStaticObject(container, object string, t *testing.T) {
	ctx := context.Background()
	err = con.StaticLargeObjectDelete(ctx, container, object)
	if err != nil {
		t.Errorf("Fail at delte dynamic Large Object: %s", err.Error())
	}
	checkNotExistObject(container, object, t)
	objs, err := con.ObjectsAll(ctx, segmentContainer, nil)
	if err != nil {
		t.Errorf("Fail at check delte dynamic Large Object: %s", err.Error())
	}
	if len(objs) != 0 {
		t.Errorf("Fail at check delte dynamic Large Object: seg not deleted")
	}
}
