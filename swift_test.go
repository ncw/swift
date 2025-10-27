// This tests the swift packagae
//
// It can be used with a real swift server which should be set up in
// the environment variables SWIFT_API_USER, SWIFT_API_KEY and
// SWIFT_AUTH_URL
// In case those variables are not defined, a fake Swift server
// is used instead - see Testing in README.md for more info
//
// The functions are designed to run in order and create things the
// next function tests.  This means that if it goes wrong it is likely
// errors will propagate.  You may need to tidy up the CONTAINER to
// get it to run cleanly.
package swift_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ncw/swift/v2"
	"github.com/ncw/swift/v2/swifttest"
)

var (
	srv              *swifttest.SwiftServer
	m1               = swift.Metadata{"Hello": "1", "potato-Salad": "2"}
	m2               = swift.Metadata{"hello": "", "potato-salad": ""}
	skipVersionTests = false
)

const (
	CONTAINER          = "GoSwiftUnitTest"
	SEGMENTS_CONTAINER = "GoSwiftUnitTest_segments"
	VERSIONS_CONTAINER = "GoSwiftUnitTestVersions"
	CURRENT_CONTAINER  = "GoSwiftUnitTestCurrent"
	OBJECT             = "test_object"
	OBJECT2            = "test_object2"
	SYMLINK_OBJECT     = "test_symlink"
	SYMLINK_OBJECT2    = "test_symlink2"
	EMPTYOBJECT        = "empty_test_object"
	CONTENTS           = "12345"
	CONTENTS2          = "54321"
	CONTENT_SIZE       = int64(len(CONTENTS))
	CONTENT_MD5        = "827ccb0eea8a706c4c34a16891f84e7b"
	CONTENT2_MD5       = "01cfcd4f6b8770febfb40cb906715822"
	EMPTY_MD5          = "d41d8cd98f00b204e9800998ecf8427e"
	SECRET_KEY         = "b3968d0207b54ece87cccc06515a89d4"
)

type someTransport struct{ http.Transport }

func makeConnection(t *testing.T) (*swift.Connection, func()) {
	var err error

	UserName := os.Getenv("SWIFT_API_USER")
	ApiKey := os.Getenv("SWIFT_API_KEY")
	AuthUrl := os.Getenv("SWIFT_AUTH_URL")
	Region := os.Getenv("SWIFT_REGION_NAME")
	EndpointType := os.Getenv("SWIFT_ENDPOINT_TYPE")

	Insecure := os.Getenv("SWIFT_AUTH_INSECURE")
	ConnectionChannelTimeout := os.Getenv("SWIFT_CONNECTION_CHANNEL_TIMEOUT")
	DataChannelTimeout := os.Getenv("SWIFT_DATA_CHANNEL_TIMEOUT")

	internalServer := false
	if UserName == "" || ApiKey == "" || AuthUrl == "" {
		srv, err = swifttest.NewSwiftServer("localhost")
		if err != nil && t != nil {
			t.Fatal("Failed to create server", err)
		}

		UserName = "swifttest"
		ApiKey = "swifttest"
		AuthUrl = srv.AuthURL
		internalServer = true
	}

	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConnsPerHost: 2048,
	}
	if Insecure == "1" {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	swift.SetExpectContinueTimeout(transport, 5*time.Second)

	c := swift.Connection{
		UserName:       UserName,
		ApiKey:         ApiKey,
		AuthUrl:        AuthUrl,
		Region:         Region,
		Transport:      transport,
		ConnectTimeout: 60 * time.Second,
		Timeout:        60 * time.Second,
		EndpointType:   swift.EndpointType(EndpointType),
	}

	if !internalServer {
		if isV3Api() {
			c.Tenant = os.Getenv("SWIFT_TENANT")
			c.Domain = os.Getenv("SWIFT_API_DOMAIN")
		} else {
			c.Tenant = os.Getenv("SWIFT_TENANT")
			c.TenantId = os.Getenv("SWIFT_TENANT_ID")
		}
	}

	var timeout int64
	if ConnectionChannelTimeout != "" {
		timeout, err = strconv.ParseInt(ConnectionChannelTimeout, 10, 32)
		if err == nil {
			c.ConnectTimeout = time.Duration(timeout) * time.Second
		}
	}

	if DataChannelTimeout != "" {
		timeout, err = strconv.ParseInt(DataChannelTimeout, 10, 32)
		if err == nil {
			c.Timeout = time.Duration(timeout) * time.Second
		}
	}

	return &c, func() {
		if srv != nil {
			srv.Close()
		}
	}
}

func makeConnectionAuth(t *testing.T) (*swift.Connection, func()) {
	ctx := context.Background()
	c, rollback := makeConnection(t)
	err := c.Authenticate(ctx)
	if err != nil {
		t.Fatal("Auth failed", err)
	}
	return c, rollback
}

func makeConnectionWithContainer(t *testing.T) (*swift.Connection, func()) {
	ctx := context.Background()
	c, rollback := makeConnectionAuth(t)
	err := c.ContainerCreate(ctx, CONTAINER, m1.ContainerHeaders())
	if err != nil {
		t.Fatal(err)
	}
	return c, func() {
		_ = c.ContainerDelete(ctx, CONTAINER)
		rollback()
	}
}

func makeConnectionWithObject(t *testing.T) (*swift.Connection, func()) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	err := c.ObjectPutString(ctx, CONTAINER, OBJECT, CONTENTS, "")
	if err != nil {
		t.Fatal(err)
	}
	return c, func() {
		_ = c.ObjectDelete(ctx, CONTAINER, OBJECT)
		rollback()
	}
}

func makeConnectionWithObjectHeaders(t *testing.T) (*swift.Connection, func()) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObject(t)
	err := c.ObjectUpdate(ctx, CONTAINER, OBJECT, m1.ObjectHeaders())
	if err != nil {
		t.Fatal(err)
	}
	return c, rollback
}

func makeConnectionWithVersionsContainer(t *testing.T) (*swift.Connection, func()) {
	ctx := context.Background()
	c, rollback := makeConnectionAuth(t)
	err := c.VersionContainerCreate(ctx, CURRENT_CONTAINER, VERSIONS_CONTAINER)
	newRollback := func() {
		_ = c.ContainerDelete(ctx, CURRENT_CONTAINER)
		_ = c.ContainerDelete(ctx, VERSIONS_CONTAINER)
		rollback()
	}
	if err != nil {
		if err == swift.Forbidden {
			skipVersionTests = true
			return c, newRollback
		}
		t.Fatal(err)
	}
	return c, newRollback
}

func makeConnectionWithVersionsObject(t *testing.T) (*swift.Connection, func()) {
	ctx := context.Background()
	c, rollback := makeConnectionWithVersionsContainer(t)
	if err := c.ObjectPutString(ctx, CURRENT_CONTAINER, OBJECT, CONTENTS, ""); err != nil {
		t.Fatal(err)
	}
	// Version 2
	if err := c.ObjectPutString(ctx, CURRENT_CONTAINER, OBJECT, CONTENTS2, ""); err != nil {
		t.Fatal(err)
	}
	// Version 3
	if err := c.ObjectPutString(ctx, CURRENT_CONTAINER, OBJECT, CONTENTS2, ""); err != nil {
		t.Fatal(err)
	}
	return c, func() {
		for i := 0; i < 3; i++ {
			_ = c.ObjectDelete(ctx, CURRENT_CONTAINER, OBJECT)
		}
		rollback()
	}
}

func makeConnectionWithSegmentsContainer(t *testing.T) (*swift.Connection, func()) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	err := c.ContainerCreate(ctx, SEGMENTS_CONTAINER, swift.Headers{})
	if err != nil {
		t.Fatal(err)
	}
	return c, func() {
		err = c.ContainerDelete(ctx, SEGMENTS_CONTAINER)
		if err != nil {
			t.Fatal(err)
		}
		rollback()
	}
}

func makeConnectionWithDLO(t *testing.T) (*swift.Connection, func()) {
	ctx := context.Background()
	c, rollback := makeConnectionWithSegmentsContainer(t)
	opts := swift.LargeObjectOpts{
		Container:   CONTAINER,
		ObjectName:  OBJECT,
		ContentType: "image/jpeg",
	}
	out, err := c.DynamicLargeObjectCreate(ctx, &opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		_, err = fmt.Fprintf(out, "%d %s\n", i, CONTENTS)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = out.CloseWithContext(ctx)
	if err != nil {
		t.Error(err)
	}
	return c, func() {
		_ = c.DynamicLargeObjectDelete(ctx, CONTAINER, OBJECT)
		rollback()
	}
}

func makeConnectionWithSLO(t *testing.T) (*swift.Connection, func()) {
	ctx := context.Background()
	c, rollback := makeConnectionWithSegmentsContainer(t)
	opts := swift.LargeObjectOpts{
		Container:   CONTAINER,
		ObjectName:  OBJECT,
		ContentType: "image/jpeg",
	}
	out, err := c.StaticLargeObjectCreate(ctx, &opts)
	if err != nil {
		if err == swift.SLONotSupported {
			t.Skip("SLO not supported")
			return c, rollback
		}
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		_, err = fmt.Fprintf(out, "%d %s\n", i, CONTENTS)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = out.CloseWithContext(ctx)
	if err != nil {
		t.Error(err)
	}
	return c, func() {
		_ = c.StaticLargeObjectDelete(ctx, CONTAINER, OBJECT)
		rollback()
	}
}

func isV3Api() bool {
	AuthUrl := os.Getenv("SWIFT_AUTH_URL")
	return strings.Contains(AuthUrl, "v3")
}

func getSwinftInfo(t *testing.T) (info swift.SwiftInfo, err error) {
	ctx := context.Background()
	c, rollback := makeConnectionAuth(t)
	defer rollback()
	return c.QueryInfo(ctx)
}

func TestTransport(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnection(t)
	defer rollback()

	tr := &someTransport{
		Transport: http.Transport{
			MaxIdleConnsPerHost: 2048,
		},
	}

	Insecure := os.Getenv("SWIFT_AUTH_INSECURE")

	if Insecure == "1" {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	c.Transport = tr

	err := c.Authenticate(ctx)
	if err != nil {
		t.Fatal("Auth failed", err)
	}
	if !c.Authenticated() {
		t.Fatal("Not authenticated")
	}
}

// The following Test functions are run in order - this one must come before the others!
func TestV1V2Authenticate(t *testing.T) {
	ctx := context.Background()
	if isV3Api() {
		return
	}
	c, rollback := makeConnection(t)
	defer rollback()

	err := c.Authenticate(ctx)
	if err != nil {
		t.Fatal("Auth failed", err)
	}
	if !c.Authenticated() {
		t.Fatal("Not authenticated")
	}
}

func TestV3AuthenticateWithDomainNameAndTenantId(t *testing.T) {
	ctx := context.Background()
	if !isV3Api() {
		return
	}

	c, rollback := makeConnection(t)
	defer rollback()

	c.Tenant = ""
	c.Domain = os.Getenv("SWIFT_API_DOMAIN")
	c.TenantId = os.Getenv("SWIFT_TENANT_ID")
	c.DomainId = ""

	err := c.Authenticate(ctx)
	if err != nil {
		t.Fatal("Auth failed", err)
	}
	if !c.Authenticated() {
		t.Fatal("Not authenticated")
	}
}

func TestV3TrustWithTrustId(t *testing.T) {
	ctx := context.Background()
	if !isV3Api() {
		return
	}

	c, rollback := makeConnection(t)
	defer rollback()

	c.TrustId = os.Getenv("SWIFT_TRUST_ID")

	err := c.Authenticate(ctx)
	if err != nil {
		t.Fatal("Auth failed", err)
	}
	if !c.Authenticated() {
		t.Fatal("Not authenticated")
	}
}

func TestV3AuthenticateWithDomainIdAndTenantId(t *testing.T) {
	ctx := context.Background()
	if !isV3Api() {
		return
	}

	c, rollback := makeConnection(t)
	defer rollback()

	c.Tenant = ""
	c.Domain = ""
	c.TenantId = os.Getenv("SWIFT_TENANT_ID")
	c.DomainId = os.Getenv("SWIFT_API_DOMAIN_ID")

	err := c.Authenticate(ctx)
	if err != nil {
		t.Fatal("Auth failed", err)
	}
	if !c.Authenticated() {
		t.Fatal("Not authenticated")
	}
}

func TestV3AuthenticateWithDomainNameAndTenantName(t *testing.T) {
	ctx := context.Background()
	if !isV3Api() {
		return
	}

	c, rollback := makeConnection(t)
	defer rollback()

	c.Tenant = os.Getenv("SWIFT_TENANT")
	c.Domain = os.Getenv("SWIFT_API_DOMAIN")
	c.TenantId = ""
	c.DomainId = ""

	err := c.Authenticate(ctx)
	if err != nil {
		t.Fatal("Auth failed", err)
	}
	if !c.Authenticated() {
		t.Fatal("Not authenticated")
	}
}

func TestV3AuthenticateWithDomainIdAndTenantName(t *testing.T) {
	ctx := context.Background()
	if !isV3Api() {
		return
	}

	c, rollback := makeConnection(t)
	defer rollback()

	c.Tenant = os.Getenv("SWIFT_TENANT")
	c.Domain = ""
	c.TenantId = ""
	c.DomainId = os.Getenv("SWIFT_API_DOMAIN_ID")

	err := c.Authenticate(ctx)
	if err != nil {
		t.Fatal("Auth failed", err)
	}
	if !c.Authenticated() {
		t.Fatal("Not authenticated")
	}
}

// Attempt to trigger a race in authenticate
//
// Run with -race to test
func TestAuthenticateRace(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnection(t)
	defer rollback()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.Authenticate(ctx)
			if err != nil {
				t.Error("Auth failed", err)
			}
			if !c.Authenticated() {
				t.Error("Not authenticated")
			}
		}()
	}
	wg.Wait()
}

// Test a connection can be serialized and unserialized with JSON
func TestSerializeConnectionJson(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionAuth(t)
	defer rollback()
	serializedConnection, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("Failed to serialize connection: %v", err)
	}
	c2 := new(swift.Connection)
	err = json.Unmarshal(serializedConnection, &c2)
	if err != nil {
		t.Fatalf("Failed to unserialize connection: %v", err)
	}
	if !c2.Authenticated() {
		t.Fatal("Should be authenticated")
	}
	_, _, err = c2.Account(ctx)
	if err != nil {
		t.Fatalf("Failed to use unserialized connection: %v", err)
	}
}

// Test a connection can be serialized and unserialized with XML
func TestSerializeConnectionXml(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionAuth(t)
	defer rollback()
	serializedConnection, err := xml.Marshal(c)
	if err != nil {
		t.Fatalf("Failed to serialize connection: %v", err)
	}
	c2 := new(swift.Connection)
	err = xml.Unmarshal(serializedConnection, &c2)
	if err != nil {
		t.Fatalf("Failed to unserialize connection: %v", err)
	}
	if !c2.Authenticated() {
		t.Fatal("Should be authenticated")
	}
	_, _, err = c2.Account(ctx)
	if err != nil {
		t.Fatalf("Failed to use unserialized connection: %v", err)
	}
}

// Test the reauthentication logic
func TestOnReAuth(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionAuth(t)
	defer rollback()
	c.UnAuthenticate()
	_, _, err := c.Account(ctx)
	if err != nil {
		t.Fatalf("Failed to reauthenticate: %v", err)
	}
}

func TestAccount(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionAuth(t)
	defer rollback()
	info, headers, err := c.Account(ctx)
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
}

func compareMaps(t *testing.T, a, b map[string]string) {
	if len(a) != len(b) {
		t.Error("Maps different sizes", a, b)
	}
	for ka, va := range a {
		if vb, ok := b[ka]; !ok || va != vb {
			t.Error("Difference in key", ka, va, b[ka])
		}
	}
	for kb, vb := range b {
		if va, ok := a[kb]; !ok || vb != va {
			t.Error("Difference in key", kb, vb, a[kb])
		}
	}
}

func TestAccountUpdate(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionAuth(t)
	defer rollback()
	err := c.AccountUpdate(ctx, m1.AccountHeaders())
	if err != nil {
		t.Fatal(err)
	}

	_, headers, err := c.Account(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m := headers.AccountMetadata()
	delete(m, "temp-url-key") // remove X-Account-Meta-Temp-URL-Key if set
	compareMaps(t, m, map[string]string{"hello": "1", "potato-salad": "2"})

	err = c.AccountUpdate(ctx, m2.AccountHeaders())
	if err != nil {
		t.Fatal(err)
	}

	_, headers, err = c.Account(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m = headers.AccountMetadata()
	delete(m, "temp-url-key") // remove X-Account-Meta-Temp-URL-Key if set
	compareMaps(t, m, map[string]string{})
}

func TestContainerCreate(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionAuth(t)
	defer rollback()
	err := c.ContainerCreate(ctx, CONTAINER, m1.ContainerHeaders())
	if err != nil {
		t.Fatal(err)
	}
	err = c.ContainerDelete(ctx, CONTAINER)
	if err != nil {
		t.Fatal(err)
	}
}

func TestContainer(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	info, headers, err := c.Container(ctx, CONTAINER)
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
}

func TestContainersAll(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	containers1, err := c.ContainersAll(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	containers2, err := c.Containers(ctx, nil)
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
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	containers1, err := c.ContainersAll(ctx, &swift.ContainersOpts{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	containers2, err := c.Containers(ctx, nil)
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
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	err := c.ContainerUpdate(ctx, CONTAINER, m2.ContainerHeaders())
	if err != nil {
		t.Fatal(err)
	}
	_, headers, err := c.Container(ctx, CONTAINER)
	if err != nil {
		t.Fatal(err)
	}
	compareMaps(t, headers.ContainerMetadata(), map[string]string{})
}

func TestContainerNames(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	containers, err := c.ContainerNames(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
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
}

func TestContainerNamesAll(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	containers1, err := c.ContainerNamesAll(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	containers2, err := c.ContainerNames(ctx, nil)
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
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	containers1, err := c.ContainerNamesAll(ctx, &swift.ContainersOpts{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	containers2, err := c.ContainerNames(ctx, nil)
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
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	err := c.ObjectPutString(ctx, CONTAINER, OBJECT, CONTENTS, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.ObjectDelete(ctx, CONTAINER, OBJECT)
		if err != nil {
			t.Fatal(err)
		}
	}()

	info, _, err := c.Object(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error(err)
	}
	if info.ContentType != "application/octet-stream" {
		t.Error("Bad content type", info.ContentType)
	}
	if info.Bytes != CONTENT_SIZE {
		t.Error("Bad length")
	}
	if info.Hash != CONTENT_MD5 {
		t.Error("Bad length")
	}
}

func TestObjectPut(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()

	headers := swift.Headers{}

	// Set content size incorrectly - should produce an error
	headers["Content-Length"] = strconv.FormatInt(CONTENT_SIZE-1, 10)
	contents := bytes.NewBufferString(CONTENTS)
	_, err := c.ObjectPut(ctx, CONTAINER, OBJECT, contents, true, CONTENT_MD5, "text/plain", headers)
	if err == nil {
		t.Fatal("Expecting error but didn't get one")
	}

	// Now set content size correctly
	contents = bytes.NewBufferString(CONTENTS)
	headers["Content-Length"] = strconv.FormatInt(CONTENT_SIZE, 10)
	h, err := c.ObjectPut(ctx, CONTAINER, OBJECT, contents, true, CONTENT_MD5, "text/plain", headers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.ObjectDelete(ctx, CONTAINER, OBJECT)
		if err != nil {
			t.Fatal(err)
		}
	}()

	if h["Etag"] != CONTENT_MD5 {
		t.Errorf("Bad Etag want %q got %q", CONTENT_MD5, h["Etag"])
	}

	// Fetch object info and compare
	info, _, err := c.Object(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error(err)
	}
	if info.ContentType != "text/plain" {
		t.Error("Bad content type", info.ContentType)
	}
	if info.Bytes != CONTENT_SIZE {
		t.Error("Bad length")
	}
	if info.Hash != CONTENT_MD5 {
		t.Error("Bad length")
	}
}

func TestObjectPutWithReauth(t *testing.T) {
	ctx := context.Background()
	if !swift.IS_AT_LEAST_GO_16 {
		return
	}
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()

	// Simulate that our auth token expired
	c.AuthToken = "expiredtoken"

	r := strings.NewReader(CONTENTS)
	_, err := c.ObjectPut(ctx, CONTAINER, OBJECT, r, false, "", "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}

	info, _, err := c.Object(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error(err)
	}
	if info.ContentType != "text/plain" {
		t.Error("Bad content type", info.ContentType)
	}
	if info.Bytes != CONTENT_SIZE {
		t.Error("Bad length")
	}
	if info.Hash != CONTENT_MD5 {
		t.Error("Bad length")
	}
}

func TestObjectPutStringWithReauth(t *testing.T) {
	ctx := context.Background()
	if !swift.IS_AT_LEAST_GO_16 {
		return
	}
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()

	// Simulate that our auth token expired
	c.AuthToken = "expiredtoken"

	err := c.ObjectPutString(ctx, CONTAINER, OBJECT, CONTENTS, "")
	if err != nil {
		t.Fatal(err)
	}

	info, _, err := c.Object(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error(err)
	}
	if info.ContentType != "application/octet-stream" {
		t.Error("Bad content type", info.ContentType)
	}
	if info.Bytes != CONTENT_SIZE {
		t.Error("Bad length")
	}
	if info.Hash != CONTENT_MD5 {
		t.Error("Bad length")
	}
}

func TestObjectEmpty(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	err := c.ObjectPutString(ctx, CONTAINER, EMPTYOBJECT, "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.ObjectDelete(ctx, CONTAINER, EMPTYOBJECT)
		if err != nil {
			t.Error(err)
		}
	}()

	info, _, err := c.Object(ctx, CONTAINER, EMPTYOBJECT)
	if err != nil {
		t.Error(err)
	}
	if info.ContentType != "application/octet-stream" {
		t.Error("Bad content type", info.ContentType)
	}
	if info.Bytes != 0 {
		t.Errorf("Bad length want 0 got %v", info.Bytes)
	}
	if info.Hash != EMPTY_MD5 {
		t.Errorf("Bad MD5 want %v got %v", EMPTY_MD5, info.Hash)
	}
}

func TestSymlinkObject(t *testing.T) {
	ctx := context.Background()
	info, err := getSwinftInfo(t)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := info["symlink"]; !ok {
		// skip, symlink not supported
		t.Skip("skip, symlink not supported")
		return
	}
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()

	// write target objects
	err = c.ObjectPutBytes(ctx, CONTAINER, OBJECT, []byte(CONTENTS), "text/potato")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.ObjectDelete(ctx, CONTAINER, OBJECT)
		if err != nil {
			t.Error(err)
		}
	}()

	// test dynamic link
	_, err = c.ObjectSymlinkCreate(ctx, CONTAINER, SYMLINK_OBJECT, "", CONTAINER, OBJECT, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.ObjectDelete(ctx, CONTAINER, SYMLINK_OBJECT)
		if err != nil {
			t.Error(err)
		}
	}()

	md, _, err := c.Object(ctx, CONTAINER, SYMLINK_OBJECT)
	if err != nil {
		t.Error(err)
	}
	if md.ContentType != "text/potato" {
		t.Error("Bad content type", md.ContentType)
	}
	if md.Bytes != CONTENT_SIZE {
		t.Errorf("Bad length want 5 got %v", md.Bytes)
	}
	if md.Hash != CONTENT_MD5 {
		t.Errorf("Bad MD5 want %v got %v", CONTENT_MD5, md.Hash)
	}

}

func TestStaticSymlinkObject(t *testing.T) {
	ctx := context.Background()
	info, err := getSwinftInfo(t)
	if err != nil {
		t.Fatal(err)
	}
	if sym, ok := info["symlink"].(map[string]interface{}); ok {
		if _, ok := sym["static_links"]; !ok {
			t.Skip("skip, static symlink not supported")
			return
		}
	} else {
		t.Skip("skip, symlink not supported")
		return
	}

	c, rollback := makeConnectionWithContainer(t)
	defer rollback()

	// write target objects
	err = c.ObjectPutBytes(ctx, CONTAINER, OBJECT2, []byte(CONTENTS2), "text/tomato")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.ObjectDelete(ctx, CONTAINER, OBJECT2)
		if err != nil {
			t.Error(err)
		}
	}()

	// test static link
	// first with the wrong target etag
	_, err = c.ObjectSymlinkCreate(ctx, CONTAINER, SYMLINK_OBJECT2, "", CONTAINER, OBJECT2, CONTENT_MD5)
	if err == nil {
		t.Error("Symlink with wrong target etag should have failed")
	}

	_, err = c.ObjectSymlinkCreate(ctx, CONTAINER, SYMLINK_OBJECT2, "", CONTAINER, OBJECT2, CONTENT2_MD5)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.ObjectDelete(ctx, CONTAINER, SYMLINK_OBJECT2)
		if err != nil {
			t.Error(err)
		}
	}()

	md, _, err := c.Object(ctx, CONTAINER, SYMLINK_OBJECT2)
	if err != nil {
		t.Error(err)
	}
	if md.ContentType != "text/tomato" {
		t.Error("Bad content type", md.ContentType)
	}
	if md.Bytes != CONTENT_SIZE {
		t.Errorf("Bad length want 5 got %v", md.Bytes)
	}
	if md.Hash != CONTENT2_MD5 {
		t.Errorf("Bad MD5 want %v got %v", CONTENT2_MD5, md.Hash)
	}
}

func TestObjectPutBytes(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	err := c.ObjectPutBytes(ctx, CONTAINER, OBJECT, []byte(CONTENTS), "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.ObjectDelete(ctx, CONTAINER, OBJECT)
		if err != nil {
			t.Error(err)
		}
	}()

	info, _, err := c.Object(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error(err)
	}
	if info.ContentType != "application/octet-stream" {
		t.Error("Bad content type", info.ContentType)
	}
	if info.Bytes != CONTENT_SIZE {
		t.Error("Bad length")
	}
	if info.Hash != CONTENT_MD5 {
		t.Error("Bad length")
	}
}

func TestObjectPutMimeType(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	err := c.ObjectPutString(ctx, CONTAINER, "test.jpg", CONTENTS, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.ObjectDelete(ctx, CONTAINER, "test.jpg")
		if err != nil {
			t.Error(err)
		}
	}()

	info, _, err := c.Object(ctx, CONTAINER, "test.jpg")
	if err != nil {
		t.Error(err)
	}
	if info.ContentType != "image/jpeg" {
		t.Error("Bad content type", info.ContentType)
	}
}

func TestObjectCreate(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	out, err := c.ObjectCreate(ctx, CONTAINER, OBJECT2, true, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.ObjectDelete(ctx, CONTAINER, OBJECT2)
		if err != nil {
			t.Error(err)
		}
	}()
	buf := &bytes.Buffer{}
	hash := md5.New()
	out2 := io.MultiWriter(out, buf, hash)
	for i := 0; i < 100; i++ {
		_, _ = fmt.Fprintf(out2, "%d %s\n", i, CONTENTS)
	}
	// Ensure Headers fails if called prematurely
	_, err = out.Headers()
	if err == nil {
		t.Error("Headers should fail if called before Close()")
	}
	err = out.Close()
	if err != nil {
		t.Error(err)
	}
	expected := buf.String()
	contents, err := c.ObjectGetString(ctx, CONTAINER, OBJECT2)
	if err != nil {
		t.Error(err)
	}
	if contents != expected {
		t.Errorf("Contents wrong, expected %q, got: %q", expected, contents)
	}

	// Ensure Headers succeeds when called after a good upload
	headers, err := out.Headers()
	if err != nil {
		t.Error(err)
	}
	if len(headers) < 1 {
		t.Error("The Headers returned by Headers() should not be empty")
	}

	// Test writing on closed file
	n, err := out.Write([]byte{0})
	if err == nil || n != 0 {
		t.Error("Expecting error and n == 0 writing on closed file", err, n)
	}

	// Now with hash instead
	out, err = c.ObjectCreate(ctx, CONTAINER, OBJECT2, false, fmt.Sprintf("%x", hash.Sum(nil)), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = out.Write(buf.Bytes())
	if err != nil {
		t.Error(err)
	}
	err = out.Close()
	if err != nil {
		t.Error(err)
	}
	contents, err = c.ObjectGetString(ctx, CONTAINER, OBJECT2)
	if err != nil {
		t.Error(err)
	}
	if contents != expected {
		t.Errorf("Contents wrong, expected %q, got: %q", expected, contents)
	}

	// Now with bad hash
	out, err = c.ObjectCreate(ctx, CONTAINER, OBJECT2, false, CONTENT_MD5, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	// FIXME: work around bug which produces 503 not 422 for empty corrupted files
	_, _ = fmt.Fprintf(out, "Sausage")
	err = out.Close()
	if !errors.Is(err, swift.ObjectCorrupted) {
		t.Error("Expecting object corrupted not", err)
	}
}

func TestObjectCreateAbort(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()

	out, err := c.ObjectCreate(ctx, CONTAINER, OBJECT2, true, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = c.ObjectDelete(ctx, CONTAINER, OBJECT2) // Ignore error
	}()

	expectedContents := "foo"
	_, err = out.Write([]byte(expectedContents))
	if err != nil {
		t.Error(err)
	}

	errAbort := fmt.Errorf("abort")
	err = out.CloseWithError(errAbort)
	if err != nil {
		t.Errorf("Unexpected error %#v", err)
	}

	_, err = c.ObjectGetString(ctx, CONTAINER, OBJECT2)
	if !errors.Is(err, swift.ObjectNotFound) {
		t.Errorf("Unexpected error: %#v", err)
	}
}

func TestObjectGetString(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObject(t)
	defer rollback()
	contents, err := c.ObjectGetString(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	if contents != CONTENTS {
		t.Error("Contents wrong")
	}
}

func TestObjectGetBytes(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObject(t)
	defer rollback()
	contents, err := c.ObjectGetBytes(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != CONTENTS {
		t.Error("Contents wrong")
	}
}

func TestObjectOpen(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObject(t)
	defer rollback()
	file, _, err := c.ObjectOpen(ctx, CONTAINER, OBJECT, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	n, err := io.Copy(&buf, file)
	if err != nil {
		t.Fatal(err)
	}
	if n != CONTENT_SIZE {
		t.Fatal("Wrong length", n, CONTENT_SIZE)
	}
	if buf.String() != CONTENTS {
		t.Error("Contents wrong")
	}
	err = file.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestObjectOpenPartial(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObject(t)
	defer rollback()
	file, _, err := c.ObjectOpen(ctx, CONTAINER, OBJECT, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	n, err := io.CopyN(&buf, file, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("Wrong length", n, CONTENT_SIZE)
	}
	if buf.String() != CONTENTS[:1] {
		t.Error("Contents wrong")
	}
	err = file.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestObjectOpenLength(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObject(t)
	defer rollback()
	file, _, err := c.ObjectOpen(ctx, CONTAINER, OBJECT, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	// FIXME ideally this would check both branches of the Length() code
	n, err := file.Length(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != CONTENT_SIZE {
		t.Fatal("Wrong length", n, CONTENT_SIZE)
	}
	err = file.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestObjectOpenNotModified(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObject(t)
	defer rollback()
	_, _, err := c.ObjectOpen(ctx, CONTAINER, OBJECT, true, swift.Headers{
		"If-None-Match": CONTENT_MD5,
	})
	if err != swift.NotModified {
		t.Fatal(err)
	}
}

func TestObjectOpenSeek(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObject(t)
	defer rollback()

	plan := []struct {
		whence int
		offset int64
		result int64
	}{
		{-1, 0, 0},
		{-1, 0, 1},
		{-1, 0, 2},
		{0, 0, 0},
		{0, 0, 0},
		{0, 1, 1},
		{0, 2, 2},
		{1, 0, 3},
		{1, -2, 2},
		{1, 1, 4},
		{2, -1, 4},
		{2, -3, 2},
		{2, -2, 3},
		{2, -5, 0},
		{2, -4, 1},
	}

	file, _, err := c.ObjectOpen(ctx, CONTAINER, OBJECT, true, nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, p := range plan {
		if p.whence >= 0 {
			var result int64
			result, err = file.Seek(ctx, p.offset, p.whence)
			if err != nil {
				t.Fatal(err, p)
			}
			if result != p.result {
				t.Fatal("Seek result was", result, "expecting", p.result, p)
			}

		}
		var buf bytes.Buffer
		var n int64
		n, err = io.CopyN(&buf, file, 1)
		if err != nil {
			t.Fatal(err, p)
		}
		if n != 1 {
			t.Fatal("Wrong length", n, p)
		}
		actual := buf.String()
		expected := CONTENTS[p.result : p.result+1]
		if actual != expected {
			t.Error("Contents wrong, expecting", expected, "got", actual, p)
		}
	}

	err = file.Close()
	if err != nil {
		t.Fatal(err)
	}
}

// Test seeking to the end to find the file size
func TestObjectOpenSeekEnd(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObject(t)
	defer rollback()
	file, _, err := c.ObjectOpen(ctx, CONTAINER, OBJECT, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	n, err := file.Seek(ctx, 0, 2) // seek to end
	if err != nil {
		t.Fatal(err)
	}
	if n != CONTENT_SIZE {
		t.Fatal("Wrong offset", n)
	}

	// Now check reading returns EOF
	buf := make([]byte, 16)
	nn, err := io.ReadFull(file, buf)
	if err != io.EOF {
		t.Fatal(err)
	}
	if nn != 0 {
		t.Fatal("wrong length", n)
	}

	// Now seek back to start and check we can read the file
	n, err = file.Seek(ctx, 0, 0) // seek to start
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("Wrong offset", n)
	}

	// read file and check contents
	buf, err = io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != CONTENTS {
		t.Fatal("wrong contents", string(buf))
	}
}

func TestObjectUpdate(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObject(t)
	defer rollback()
	err := c.ObjectUpdate(ctx, CONTAINER, OBJECT, m1.ObjectHeaders())
	if err != nil {
		t.Fatal(err)
	}
}

func checkTime(t *testing.T, when time.Time, low, high int) {
	dt := time.Since(when)
	if dt < time.Duration(low)*time.Second || dt > time.Duration(high)*time.Second {
		t.Errorf("Time is wrong: dt=%q, when=%q", dt, when)
	}
}

func TestObject(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	object, headers, err := c.Object(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	compareMaps(t, headers.ObjectMetadata(), map[string]string{"hello": "1", "potato-salad": "2"})
	if object.Name != OBJECT || object.Bytes != CONTENT_SIZE || object.ContentType != "application/octet-stream" || object.Hash != CONTENT_MD5 || object.PseudoDirectory != false || object.SubDir != "" {
		t.Error("Bad object info", object)
	}
	checkTime(t, object.LastModified, -10, 10)
}

func TestObjectUpdate2(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	err := c.ObjectUpdate(ctx, CONTAINER, OBJECT, m2.ObjectHeaders())
	if err != nil {
		t.Fatal(err)
	}
	_, headers, err := c.Object(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	compareMaps(t, headers.ObjectMetadata(), map[string]string{"hello": "", "potato-salad": ""})
}

func TestContainers(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	containers, err := c.Containers(ctx, nil)
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
}

func TestObjectNames(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	objects, err := c.ObjectNames(ctx, CONTAINER, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0] != OBJECT {
		t.Error("Incorrect listing", objects)
	}
}

func TestObjectNamesAll(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	objects, err := c.ObjectNamesAll(ctx, CONTAINER, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0] != OBJECT {
		t.Error("Incorrect listing", objects)
	}
}

func TestObjectNamesAllWithLimit(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	objects, err := c.ObjectNamesAll(ctx, CONTAINER, &swift.ObjectsOpts{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0] != OBJECT {
		t.Error("Incorrect listing", objects)
	}
}

func TestObjectsWalk(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	objects := make([]string, 0)
	err := c.ObjectsWalk(ctx, container, nil, func(ctx context.Context, opts *swift.ObjectsOpts) (interface{}, error) {
		newObjects, err := c.ObjectNames(ctx, CONTAINER, opts)
		if err == nil {
			objects = append(objects, newObjects...)
		}
		return newObjects, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0] != OBJECT {
		t.Error("Incorrect listing", objects)
	}
}

func TestObjects(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	objects, err := c.Objects(ctx, CONTAINER, &swift.ObjectsOpts{Delimiter: '/'})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Fatal("Should only be 1 object")
	}
	object := objects[0]
	if object.Name != OBJECT || object.Bytes != CONTENT_SIZE || object.ContentType != "application/octet-stream" || object.Hash != CONTENT_MD5 || object.PseudoDirectory != false || object.SubDir != "" {
		t.Error("Bad object info", object)
	}
	checkTime(t, object.LastModified, -10, 10)
}

func TestObjectsDirectory(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	err := c.ObjectPutString(ctx, CONTAINER, "directory", "", "application/directory")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = c.ObjectDelete(ctx, CONTAINER, "directory")
	}()

	// Look for the directory object and check we aren't confusing
	// it with a pseudo directory object
	objects, err := c.Objects(ctx, CONTAINER, &swift.ObjectsOpts{Delimiter: '/'})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 2 {
		t.Fatal("Should only be 2 objects")
	}
	found := false
	for i := range objects {
		object := objects[i]
		if object.Name == "directory" {
			found = true
			if object.Bytes != 0 || object.ContentType != "application/directory" || object.Hash != "d41d8cd98f00b204e9800998ecf8427e" || object.PseudoDirectory != false || object.SubDir != "" {
				t.Error("Bad object info", object)
			}
			checkTime(t, object.LastModified, -10, 10)
		}
	}
	if !found {
		t.Error("Didn't find directory object")
	}
}

func TestObjectsPseudoDirectory(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	err := c.ObjectPutString(ctx, CONTAINER, "directory/puppy.jpg", "cute puppy", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = c.ObjectDelete(ctx, CONTAINER, "directory/puppy.jpg")
	}()

	// Look for the pseudo directory
	objects, err := c.Objects(ctx, CONTAINER, &swift.ObjectsOpts{Delimiter: '/'})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 2 {
		t.Fatal("Should only be 2 objects", objects)
	}
	found := false
	for i := range objects {
		object := objects[i]
		if object.Name == "directory/" {
			found = true
			if object.Bytes != 0 || object.ContentType != "application/directory" || object.Hash != "" || object.PseudoDirectory != true || object.SubDir != "directory/" && object.LastModified.IsZero() {
				t.Error("Bad object info", object)
			}
		}
	}
	if !found {
		t.Error("Didn't find directory object", objects)
	}

	// Look in the pseudo directory now
	objects, err = c.Objects(ctx, CONTAINER, &swift.ObjectsOpts{Delimiter: '/', Path: "directory/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Fatal("Should only be 1 object", objects)
	}
	object := objects[0]
	if object.Name != "directory/puppy.jpg" || object.Bytes != 10 || object.ContentType != "image/jpeg" || object.Hash != "87a12ea22fca7f54f0cefef1da535489" || object.PseudoDirectory != false || object.SubDir != "" {
		t.Error("Bad object info", object)
	}
	checkTime(t, object.LastModified, -10, 10)
}

func TestObjectsAll(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	objects, err := c.ObjectsAll(ctx, CONTAINER, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0].Name != OBJECT {
		t.Error("Incorrect listing", objects)
	}
}

func TestObjectsAllWithLimit(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	objects, err := c.ObjectsAll(ctx, CONTAINER, &swift.ObjectsOpts{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0].Name != OBJECT {
		t.Error("Incorrect listing", objects)
	}
}

func TestObjectNamesWithPath(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	objects, err := c.ObjectNames(ctx, CONTAINER, &swift.ObjectsOpts{Delimiter: '/', Path: ""})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0] != OBJECT {
		t.Error("Bad listing with path", objects)
	}
	// fmt.Println(objects)
	objects, err = c.ObjectNames(ctx, CONTAINER, &swift.ObjectsOpts{Delimiter: '/', Path: "Downloads/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 0 {
		t.Error("Bad listing with path", objects)
	}
}

func TestObjectCopy(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	_, err := c.ObjectCopy(ctx, CONTAINER, OBJECT, CONTAINER, OBJECT2, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = c.ObjectDelete(ctx, CONTAINER, OBJECT2)
	if err != nil {
		t.Fatal(err)
	}
}

func TestObjectCopyDifficultName(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	const dest = OBJECT + "?param %30%31%32 £100"
	_, err := c.ObjectCopy(ctx, CONTAINER, OBJECT, CONTAINER, dest, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = c.ObjectDelete(ctx, CONTAINER, dest)
	if err != nil {
		t.Fatal(err)
	}
}

func TestObjectCopyWithMetadata(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	m := swift.Metadata{}
	m["copy-special-metadata"] = "hello"
	m["hello"] = "9"
	h := m.ObjectHeaders()
	h["Content-Type"] = "image/jpeg"
	_, err := c.ObjectCopy(ctx, CONTAINER, OBJECT, CONTAINER, OBJECT2, h)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.ObjectDelete(ctx, CONTAINER, OBJECT2)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// Re-read the metadata to see if it is correct
	_, headers, err := c.Object(ctx, CONTAINER, OBJECT2)
	if err != nil {
		t.Fatal(err)
	}
	if headers["Content-Type"] != "image/jpeg" {
		t.Error("Didn't change content type")
	}
	compareMaps(t, headers.ObjectMetadata(), map[string]string{"hello": "9", "potato-salad": "2", "copy-special-metadata": "hello"})
}

func TestObjectMove(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	err := c.ObjectMove(ctx, CONTAINER, OBJECT, CONTAINER, OBJECT2)
	if err != nil {
		t.Fatal(err)
	}
	testExistenceAfterDelete(t, c, CONTAINER, OBJECT)
	_, _, err = c.Object(ctx, CONTAINER, OBJECT2)
	if err != nil {
		t.Fatal(err)
	}

	err = c.ObjectMove(ctx, CONTAINER, OBJECT2, CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	testExistenceAfterDelete(t, c, CONTAINER, OBJECT2)
	_, headers, err := c.Object(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	compareMaps(t, headers.ObjectMetadata(), map[string]string{"hello": "1", "potato-salad": "2"})
}

func TestObjectUpdateContentType(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObjectHeaders(t)
	defer rollback()
	err := c.ObjectUpdateContentType(ctx, CONTAINER, OBJECT, "text/potato")
	if err != nil {
		t.Fatal(err)
	}
	// Re-read the metadata to see if it is correct
	_, headers, err := c.Object(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	if headers["Content-Type"] != "text/potato" {
		t.Error("Didn't change content type")
	}
	compareMaps(t, headers.ObjectMetadata(), map[string]string{"hello": "1", "potato-salad": "2"})
}

func TestVersionContainerCreate(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionAuth(t)
	defer rollback()
	err := c.VersionContainerCreate(ctx, CURRENT_CONTAINER, VERSIONS_CONTAINER)
	defer func() {
		_ = c.ContainerDelete(ctx, CURRENT_CONTAINER)
		_ = c.ContainerDelete(ctx, VERSIONS_CONTAINER)
	}()
	if err != nil {
		if err == swift.Forbidden {
			t.Log("Server doesn't support Versions - skipping test")
			return
		}
		t.Fatal(err)
	}
}

func TestVersionObjectAdd(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithVersionsContainer(t)
	defer rollback()
	if skipVersionTests {
		t.Log("Server doesn't support Versions - skipping test")
		return
	}
	// Version 1
	if err := c.ObjectPutString(ctx, CURRENT_CONTAINER, OBJECT, CONTENTS, ""); err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := c.ObjectDelete(ctx, CURRENT_CONTAINER, OBJECT)
		if err != nil {
			t.Fatal(err)
		}
	}()
	if contents, err := c.ObjectGetString(ctx, CURRENT_CONTAINER, OBJECT); err != nil {
		t.Fatal(err)
	} else if contents != CONTENTS {
		t.Error("Contents wrong")
	}

	// Version 2
	if err := c.ObjectPutString(ctx, CURRENT_CONTAINER, OBJECT, CONTENTS2, ""); err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := c.ObjectDelete(ctx, CURRENT_CONTAINER, OBJECT)
		if err != nil {
			t.Fatal(err)
		}
	}()
	if contents, err := c.ObjectGetString(ctx, CURRENT_CONTAINER, OBJECT); err != nil {
		t.Fatal(err)
	} else if contents != CONTENTS2 {
		t.Error("Contents wrong")
	}

	// Version 3
	if err := c.ObjectPutString(ctx, CURRENT_CONTAINER, OBJECT, CONTENTS2, ""); err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := c.ObjectDelete(ctx, CURRENT_CONTAINER, OBJECT)
		if err != nil {
			t.Fatal(err)
		}
	}()
}

func TestVersionObjectList(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithVersionsObject(t)
	defer rollback()
	if skipVersionTests {
		t.Log("Server doesn't support Versions - skipping test")
		return
	}
	list, err := c.VersionObjectList(ctx, VERSIONS_CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}

	if len(list) != 2 {
		t.Error("Version list should return 2 objects")
	}
}

func TestVersionObjectDelete(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithVersionsObject(t)
	defer rollback()
	if skipVersionTests {
		t.Log("Server doesn't support Versions - skipping test")
		return
	}
	// Delete Version 3
	if err := c.ObjectDelete(ctx, CURRENT_CONTAINER, OBJECT); err != nil {
		t.Fatal(err)
	}

	// Delete Version 2
	if err := c.ObjectDelete(ctx, CURRENT_CONTAINER, OBJECT); err != nil {
		t.Fatal(err)
	}

	// Contents should be reverted to Version 1
	if contents, err := c.ObjectGetString(ctx, CURRENT_CONTAINER, OBJECT); err != nil {
		t.Fatal(err)
	} else if contents != CONTENTS {
		t.Error("Contents wrong")
	}
}

func TestVersionDeleteContent(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithVersionsObject(t)
	defer rollback()
	if skipVersionTests {
		t.Log("Server doesn't support Versions - skipping test")
		return
	}
	// Delete Version 3
	if err := c.ObjectDelete(ctx, CURRENT_CONTAINER, OBJECT); err != nil {
		t.Fatal(err)
	}
	// Delete Version 2
	if err := c.ObjectDelete(ctx, CURRENT_CONTAINER, OBJECT); err != nil {
		t.Fatal(err)
	}
	// Delete Version 1
	if err := c.ObjectDelete(ctx, CURRENT_CONTAINER, OBJECT); err != nil {
		t.Fatal(err)
	}
	if err := c.ObjectDelete(ctx, CURRENT_CONTAINER, OBJECT); !errors.Is(err, swift.ObjectNotFound) {
		t.Fatalf("Expecting Object not found error, got: %v", err)
	}
}

// Check for non existence after delete
// May have to do it a few times to wait for swift to be consistent.
func testExistenceAfterDelete(t *testing.T, c *swift.Connection, container, object string) {
	ctx := context.Background()
	for i := 10; i <= 0; i-- {
		_, _, err := c.Object(ctx, container, object)
		if errors.Is(err, swift.ObjectNotFound) {
			break
		}
		if i == 0 {
			t.Fatalf("Expecting object %q/%q not found not: err=%v", container, object, err)
		}
		time.Sleep(1 * time.Second)
	}
}

func TestObjectDelete(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithObject(t)
	defer rollback()
	err := c.ObjectDelete(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	testExistenceAfterDelete(t, c, CONTAINER, OBJECT)
	err = c.ObjectDelete(ctx, CONTAINER, OBJECT)
	if !errors.Is(err, swift.ObjectNotFound) {
		t.Fatal("Expecting Object not found", err)
	}
}

func TestBulkDelete(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	result, err := c.BulkDelete(ctx, CONTAINER, []string{OBJECT})
	if errors.Is(err, swift.Forbidden) {
		t.Log("Server doesn't support BulkDelete - skipping test")
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if result.NumberNotFound != 1 {
		t.Error("Expected 1, actual:", result.NumberNotFound)
	}
	if result.NumberDeleted != 0 {
		t.Error("Expected 0, actual:", result.NumberDeleted)
	}
	err = c.ObjectPutString(ctx, CONTAINER, OBJECT, CONTENTS, "")
	if err != nil {
		t.Fatal(err)
	}
	result, err = c.BulkDelete(ctx, CONTAINER, []string{OBJECT2, OBJECT})
	if err != nil {
		t.Fatal(err)
	}
	if result.NumberNotFound != 1 {
		t.Error("Expected 1, actual:", result.NumberNotFound)
	}
	if result.NumberDeleted != 1 {
		t.Error("Expected 1, actual:", result.NumberDeleted)
	}
	t.Log("Errors:", result.Errors)
}

func TestBulkUpload(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	buffer := new(bytes.Buffer)
	ds := tar.NewWriter(buffer)
	var files = []struct{ Name, Body string }{
		{OBJECT, CONTENTS},
		{OBJECT2, CONTENTS2},
	}
	for _, file := range files {
		hdr := &tar.Header{
			Name: file.Name,
			Size: int64(len(file.Body)),
		}
		if err := ds.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := ds.Write([]byte(file.Body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := ds.Close(); err != nil {
		t.Fatal(err)
	}

	result, err := c.BulkUpload(ctx, CONTAINER, buffer, swift.UploadTar, nil)
	if err == swift.Forbidden {
		t.Log("Server doesn't support BulkUpload - skipping test")
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.ObjectDelete(ctx, CONTAINER, OBJECT)
		if err != nil {
			t.Fatal(err)
		}
		err = c.ObjectDelete(ctx, CONTAINER, OBJECT2)
		if err != nil {
			t.Fatal(err)
		}
	}()
	if result.NumberCreated != 2 {
		t.Error("Expected 2, actual:", result.NumberCreated)
	}
	t.Log("Errors:", result.Errors)

	_, _, err = c.Object(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error("Expecting object to be found")
	}
	_, _, err = c.Object(ctx, CONTAINER, OBJECT2)
	if err != nil {
		t.Error("Expecting object to be found")
	}
}

func TestObjectDifficultName(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	const name = `hello? sausage/êé/Hello, 世界/ " ' @ < > & ?/`
	err := c.ObjectPutString(ctx, CONTAINER, name, CONTENTS, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.ObjectDelete(ctx, CONTAINER, name)
		if err != nil {
			t.Fatal(err)
		}
	}()
	objects, err := c.ObjectNamesAll(ctx, CONTAINER, nil)
	if err != nil {
		t.Error(err)
	}
	found := false
	for _, object := range objects {
		if object == name {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Couldn't find %q in listing %q", name, objects)
	}
}

func TestTempUrl(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	err := c.ObjectPutBytes(ctx, CONTAINER, OBJECT, []byte(CONTENTS), "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.ObjectDelete(ctx, CONTAINER, OBJECT)
		if err != nil {
			t.Fatal(err)
		}
	}()

	m := swift.Metadata{}
	m["temp-url-key"] = SECRET_KEY
	err = c.AccountUpdate(ctx, m.AccountHeaders())
	if err != nil {
		t.Fatal(err)
	}

	expiresTime := time.Now().Add(20 * time.Minute)
	tempUrl := c.ObjectTempUrl(CONTAINER, OBJECT, SECRET_KEY, "GET", expiresTime)
	resp, err := http.Get(tempUrl)
	if err != nil {
		t.Fatal("Failed to retrieve file from temporary url")
	}
	defer func() {
		err := resp.Body.Close()
		if err != nil {
			t.Error("Close failed", err)
		}
	}()
	if resp.StatusCode == 401 {
		t.Log("Server doesn't support tempurl")
	} else if resp.StatusCode != 200 {
		t.Fatal("HTTP Error retrieving file from temporary url", resp.StatusCode)
	} else {
		var content []byte
		if content, err = io.ReadAll(resp.Body); err != nil || string(content) != CONTENTS {
			t.Error("Bad content", err)
		}

		resp, err = http.Post(tempUrl, "image/jpeg", bytes.NewReader([]byte(CONTENTS)))
		if err != nil {
			t.Fatal("Failed to retrieve file from temporary url")
		}
		defer func() {
			err := resp.Body.Close()
			if err != nil {
				t.Error("Close failed", err)
			}
		}()
		if resp.StatusCode != 401 {
			t.Fatal("Expecting server to forbid access to object")
		}
	}
}

func TestQueryInfo(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionAuth(t)
	defer rollback()
	infos, err := c.QueryInfo(ctx)
	if err != nil {
		t.Log("Server doesn't support querying info")
		return
	}
	if _, ok := infos["swift"]; !ok {
		t.Fatal("No 'swift' section found in configuration")
	}
}

func TestDLOCreate(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithSegmentsContainer(t)
	defer rollback()

	opts := swift.LargeObjectOpts{
		Container:   CONTAINER,
		ObjectName:  OBJECT,
		ContentType: "image/jpeg",
	}
	out, err := c.DynamicLargeObjectCreate(ctx, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.DynamicLargeObjectDelete(ctx, CONTAINER, OBJECT)
		if err != nil {
			t.Fatal(err)
		}
	}()

	buf := &bytes.Buffer{}
	multi := io.MultiWriter(buf, out)
	for i := 0; i < 2; i++ {
		_, err = fmt.Fprintf(multi, "%d %s\n", i, CONTENTS)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = out.CloseWithContext(ctx)
	if err != nil {
		t.Error(err)
	}
	expected := buf.String()
	contents, err := c.ObjectGetString(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error(err)
	}
	if contents != expected {
		t.Errorf("Contents wrong, expected %q, got: %q", expected, contents)
	}
	info, _, err := c.Object(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	if info.ObjectType != swift.DynamicLargeObjectType {
		t.Errorf("Wrong ObjectType, expected %d, got: %d", swift.DynamicLargeObjectType, info.ObjectType)
	}
	if info.Bytes != int64(len(expected)) {
		t.Errorf("Wrong Bytes size, expected %d, got: %d", len(expected), info.Bytes)
	}
}

func TestDLOInsert(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithDLO(t)
	defer rollback()
	opts := swift.LargeObjectOpts{
		Container:   CONTAINER,
		ObjectName:  OBJECT,
		CheckHash:   true,
		ContentType: "image/jpeg",
	}
	out, err := c.DynamicLargeObjectCreateFile(ctx, &opts)
	if err != nil {
		t.Fatal(err)
	}

	buf := &bytes.Buffer{}
	multi := io.MultiWriter(buf, out)
	_, err = fmt.Fprintf(multi, "%d%s\n", 0, CONTENTS)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fmt.Fprintf(buf, "\n%d %s\n", 1, CONTENTS)
	err = out.CloseWithContext(ctx)
	if err != nil {
		t.Error(err)
	}
	expected := buf.String()
	contents, err := c.ObjectGetString(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error(err)
	}
	if contents != expected {
		t.Errorf("Contents wrong, expected %q, got: %q", expected, contents)
	}
}

func TestDLOAppend(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithDLO(t)
	defer rollback()
	opts := swift.LargeObjectOpts{
		Container:   CONTAINER,
		ObjectName:  OBJECT,
		Flags:       os.O_APPEND,
		CheckHash:   true,
		ContentType: "image/jpeg",
	}
	out, err := c.DynamicLargeObjectCreateFile(ctx, &opts)
	if err != nil {
		t.Fatal(err)
	}

	contents, err := c.ObjectGetString(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	buf := bytes.NewBuffer([]byte(contents))
	multi := io.MultiWriter(buf, out)
	for i := 0; i < 2; i++ {
		_, err = fmt.Fprintf(multi, "%d %s\n", i+10, CONTENTS)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = out.CloseWithContext(ctx)
	if err != nil {
		t.Error(err)
	}
	expected := buf.String()
	contents, err = c.ObjectGetString(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error(err)
	}
	if contents != expected {
		t.Errorf("Contents wrong, expected %q, got: %q", expected, contents)
	}
}

func TestDLOTruncate(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithDLO(t)
	defer rollback()
	opts := swift.LargeObjectOpts{
		Container:   CONTAINER,
		ObjectName:  OBJECT,
		Flags:       os.O_TRUNC,
		CheckHash:   true,
		ContentType: "image/jpeg",
	}
	out, err := c.DynamicLargeObjectCreateFile(ctx, &opts)
	if err != nil {
		t.Fatal(err)
	}

	buf := &bytes.Buffer{}
	multi := io.MultiWriter(buf, out)
	_, err = fmt.Fprintf(multi, "%s", CONTENTS)
	if err != nil {
		t.Fatal(err)
	}
	err = out.CloseWithContext(ctx)
	if err != nil {
		t.Error(err)
	}
	expected := buf.String()
	contents, err := c.ObjectGetString(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error(err)
	}
	if contents != expected {
		t.Errorf("Contents wrong, expected %q, got: %q", expected, contents)
	}
}

func TestDLOMove(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithDLO(t)
	defer rollback()
	contents, err := c.ObjectGetString(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DynamicLargeObjectMove(ctx, CONTAINER, OBJECT, CONTAINER, OBJECT2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.DynamicLargeObjectDelete(ctx, CONTAINER, OBJECT2)
		if err != nil {
			t.Fatal(err)
		}
	}()

	contents2, err := c.ObjectGetString(ctx, CONTAINER, OBJECT2)
	if err != nil {
		t.Fatal(err)
	}

	if contents2 != contents {
		t.Error("Contents wrong")
	}
}

func TestDLONoSegmentContainer(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithDLO(t)
	defer rollback()
	opts := swift.LargeObjectOpts{
		Container:        CONTAINER,
		ObjectName:       OBJECT,
		ContentType:      "image/jpeg",
		SegmentContainer: CONTAINER,
	}
	out, err := c.DynamicLargeObjectCreate(ctx, &opts)
	if err != nil {
		t.Fatal(err)
	}

	buf := &bytes.Buffer{}
	multi := io.MultiWriter(buf, out)
	for i := 0; i < 2; i++ {
		_, err = fmt.Fprintf(multi, "%d %s\n", i, CONTENTS)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = out.CloseWithContext(ctx)
	if err != nil {
		t.Error(err)
	}
	expected := buf.String()
	contents, err := c.ObjectGetString(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error(err)
	}
	if contents != expected {
		t.Errorf("Contents wrong, expected %q, got: %q", expected, contents)
	}
}

func TestDLOCreateMissingSegmentsInList(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()

	if srv == nil {
		t.Skipf("This test only runs with the fake swift server as it's needed to simulate eventual consistency problems.")
		return
	}

	listURL := "/v1/AUTH_" + swifttest.TEST_ACCOUNT + "/" + SEGMENTS_CONTAINER
	srv.SetOverride(listURL, func(w http.ResponseWriter, r *http.Request, recorder *httptest.ResponseRecorder) {
		for k, v := range recorder.Result().Header {
			w.Header().Set(k, v[0])
		}
		w.WriteHeader(recorder.Code)
		_, _ = w.Write([]byte("null\n"))
	})
	defer srv.UnsetOverride(listURL)

	headers := swift.Headers{}
	err := c.ContainerCreate(ctx, SEGMENTS_CONTAINER, headers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.ContainerDelete(ctx, SEGMENTS_CONTAINER)
		if err != nil {
			t.Fatal(err)
		}
	}()

	opts := swift.LargeObjectOpts{
		Container:   CONTAINER,
		ObjectName:  OBJECT,
		ContentType: "image/jpeg",
	}
	out, err := c.DynamicLargeObjectCreate(ctx, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.DynamicLargeObjectDelete(ctx, CONTAINER, OBJECT)
		if err != nil {
			t.Fatal(err)
		}
	}()

	buf := &bytes.Buffer{}
	multi := io.MultiWriter(buf, out)
	for i := 0; i < 2; i++ {
		_, err = fmt.Fprintf(multi, "%d %s\n", i, CONTENTS)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = out.CloseWithContext(ctx)
	if err != nil {
		t.Error(err)
	}
	expected := buf.String()
	contents, err := c.ObjectGetString(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error(err)
	}
	if contents != expected {
		t.Errorf("Contents wrong, expected %q, got: %q", expected, contents)
	}
}

func TestDLOCreateIncorrectSize(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()

	if srv == nil {
		t.Skipf("This test only runs with the fake swift server as it's needed to simulate eventual consistency problems.")
		return
	}

	listURL := "/v1/AUTH_" + swifttest.TEST_ACCOUNT + "/" + CONTAINER + "/" + OBJECT
	headCount := 0
	expectedHeadCount := 5
	srv.SetOverride(listURL, func(w http.ResponseWriter, r *http.Request, recorder *httptest.ResponseRecorder) {
		for k, v := range recorder.Result().Header {
			w.Header().Set(k, v[0])
		}
		if r.Method == "HEAD" {
			headCount++
			if headCount < expectedHeadCount {
				w.Header().Set("Content-Length", "7")
			}
		}
		w.WriteHeader(recorder.Code)
		_, _ = w.Write(recorder.Body.Bytes())
	})
	defer srv.UnsetOverride(listURL)

	headers := swift.Headers{}
	err := c.ContainerCreate(ctx, SEGMENTS_CONTAINER, headers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.ContainerDelete(ctx, SEGMENTS_CONTAINER)
		if err != nil {
			t.Fatal(err)
		}
	}()

	opts := swift.LargeObjectOpts{
		Container:   CONTAINER,
		ObjectName:  OBJECT,
		ContentType: "image/jpeg",
	}
	out, err := c.DynamicLargeObjectCreate(ctx, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.DynamicLargeObjectDelete(ctx, CONTAINER, OBJECT)
		if err != nil {
			t.Fatal(err)
		}
	}()
	buf := &bytes.Buffer{}
	multi := io.MultiWriter(buf, out)
	for i := 0; i < 2; i++ {
		_, err = fmt.Fprintf(multi, "%d %s\n", i, CONTENTS)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = out.CloseWithContext(ctx)
	if err != nil {
		t.Error(err)
	}
	if headCount != expectedHeadCount {
		t.Errorf("Unexpected HEAD requests count, expected %d, got: %d", expectedHeadCount, headCount)
	}
	expected := buf.String()
	contents, err := c.ObjectGetString(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error(err)
	}
	if contents != expected {
		t.Errorf("Contents wrong, expected %q, got: %q", expected, contents)
	}
}

func TestDLOConcurrentWrite(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithSegmentsContainer(t)
	defer rollback()

	nConcurrency := 5
	nChunks := 100
	var chunkSize int64 = 1024

	writeFn := func(i int) {
		objName := fmt.Sprintf("%s_concurrent_dlo_%d", OBJECT, i)
		opts := swift.LargeObjectOpts{
			Container:   CONTAINER,
			ObjectName:  objName,
			ContentType: "image/jpeg",
		}
		out, err := c.DynamicLargeObjectCreate(ctx, &opts)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			err = c.DynamicLargeObjectDelete(ctx, CONTAINER, objName)
			if err != nil {
				t.Fatal(err)
			}
		}()
		buf := &bytes.Buffer{}
		for j := 0; j < nChunks; j++ {
			var data []byte
			var n int
			data, err = io.ReadAll(io.LimitReader(rand.Reader, chunkSize))
			if err != nil {
				t.Fatal(err)
			}
			multi := io.MultiWriter(buf, out)
			n, err = multi.Write(data)
			if err != nil {
				t.Fatal(err)
			}
			if int64(n) != chunkSize {
				t.Fatalf("expected to write %d, got: %d", chunkSize, n)
			}
		}
		err = out.CloseWithContext(ctx)
		if err != nil {
			t.Error(err)
		}
		expected := buf.String()
		contents, err := c.ObjectGetString(ctx, CONTAINER, objName)
		if err != nil {
			t.Error(err)
		}
		if contents != expected {
			t.Error("Contents wrong")
		}
	}

	wg := sync.WaitGroup{}
	for i := 0; i < nConcurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			writeFn(i)
		}(i)
	}
	wg.Wait()
}

func TestDLOSegmentation(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithSegmentsContainer(t)
	defer rollback()

	opts := swift.LargeObjectOpts{
		Container:   CONTAINER,
		ObjectName:  OBJECT,
		ContentType: "image/jpeg",
		ChunkSize:   6,
		NoBuffer:    true,
	}

	testSegmentation(t, c, func() swift.LargeObjectFile {
		out, err := c.DynamicLargeObjectCreate(ctx, &opts)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}, []segmentTest{
		{
			writes:        []string{"0", "1", "2", "3", "4", "5", "6", "7", "8"},
			expectedSegs:  []string{"0", "1", "2", "3", "4", "5", "6", "7", "8"},
			expectedValue: "012345678",
		},
		{
			writes:        []string{"012345", "012345"},
			expectedSegs:  []string{"012345", "012345"},
			expectedValue: "012345012345",
		},
		{
			writes:        []string{"0123456", "0123456"},
			expectedSegs:  []string{"012345", "6", "012345", "6"},
			expectedValue: "01234560123456",
		},
		{
			writes:        []string{"0123456", "0123456"},
			seeks:         []int{-4, 0},
			expectedSegs:  []string{"012012", "3456"},
			expectedValue: "0120123456",
		},
		{
			writes:        []string{"0123456", "0123456", "abcde"},
			seeks:         []int{0, -11, 0},
			expectedSegs:  []string{"012abc", "d", "e12345", "6"},
			expectedValue: "012abcde123456",
		},
		{
			writes:        []string{"0123456", "ab"},
			seeks:         []int{-4, 0},
			expectedSegs:  []string{"012ab5", "6"},
			expectedValue: "012ab56",
		},
	})
}

func TestDLOSegmentationBuffered(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithSegmentsContainer(t)
	defer rollback()

	opts := swift.LargeObjectOpts{
		Container:   CONTAINER,
		ObjectName:  OBJECT,
		ContentType: "image/jpeg",
		ChunkSize:   6,
	}

	testSegmentation(t, c, func() swift.LargeObjectFile {
		out, err := c.DynamicLargeObjectCreate(ctx, &opts)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}, []segmentTest{
		{
			writes:        []string{"0", "1", "2", "3", "4", "5", "6", "7", "8"},
			expectedSegs:  []string{"012345", "678"},
			expectedValue: "012345678",
		},
		{
			writes:        []string{"012345", "012345"},
			expectedSegs:  []string{"012345", "012345"},
			expectedValue: "012345012345",
		},
		{
			writes:        []string{"0123456", "0123456"},
			expectedSegs:  []string{"012345", "6", "012345", "6"},
			expectedValue: "01234560123456",
		},
		{
			writes:        []string{"0123456", "0123456"},
			seeks:         []int{-4, 0},
			expectedSegs:  []string{"012012", "3456"},
			expectedValue: "0120123456",
		},
		{
			writes:        []string{"0123456", "0123456", "abcde"},
			seeks:         []int{0, -11, 0},
			expectedSegs:  []string{"012abc", "d", "e12345", "6"},
			expectedValue: "012abcde123456",
		},
		{
			writes:        []string{"0123456", "ab"},
			seeks:         []int{-4, 0},
			expectedSegs:  []string{"012ab5", "6"},
			expectedValue: "012ab56",
		},
	})
}

func TestSLOCreate(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithSegmentsContainer(t)
	defer rollback()

	opts := swift.LargeObjectOpts{
		Container:   CONTAINER,
		ObjectName:  OBJECT,
		ContentType: "image/jpeg",
	}
	out, err := c.StaticLargeObjectCreate(ctx, &opts)
	if err != nil {
		if err == swift.SLONotSupported {
			t.Skip("SLO not supported")
			return
		}
		t.Fatal(err)
	}
	defer func() {
		err = c.StaticLargeObjectDelete(ctx, CONTAINER, OBJECT)
		if err != nil {
			t.Fatal(err)
		}
	}()

	buf := &bytes.Buffer{}
	multi := io.MultiWriter(buf, out)
	for i := 0; i < 2; i++ {
		_, err = fmt.Fprintf(multi, "%d %s\n", i, CONTENTS)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = out.CloseWithContext(ctx)
	if err != nil {
		t.Error(err)
	}
	expected := buf.String()
	contents, err := c.ObjectGetString(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error(err)
	}
	if contents != expected {
		t.Errorf("Contents wrong, expected %q, got: %q", expected, contents)
	}
	info, _, err := c.Object(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	if info.ObjectType != swift.StaticLargeObjectType {
		t.Errorf("Wrong ObjectType, expected %d, got: %d", swift.StaticLargeObjectType, info.ObjectType)
	}
	if info.Bytes != int64(len(expected)) {
		t.Errorf("Wrong Bytes size, expected %d, got: %d", len(expected), info.Bytes)
	}
}

func TestSLOInsert(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithSLO(t)
	defer rollback()
	opts := swift.LargeObjectOpts{
		Container:   CONTAINER,
		ObjectName:  OBJECT,
		ContentType: "image/jpeg",
	}
	out, err := c.StaticLargeObjectCreateFile(ctx, &opts)
	if err != nil {
		t.Fatal(err)
	}

	buf := &bytes.Buffer{}
	multi := io.MultiWriter(buf, out)
	_, err = fmt.Fprintf(multi, "%d%s\n", 0, CONTENTS)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fmt.Fprintf(buf, "\n%d %s\n", 1, CONTENTS)
	err = out.CloseWithContext(ctx)
	if err != nil {
		t.Error(err)
	}
	expected := buf.String()
	contents, err := c.ObjectGetString(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error(err)
	}
	if contents != expected {
		t.Errorf("Contents wrong, expected %q, got: %q", expected, contents)
	}
}

func TestSLOAppend(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithSLO(t)
	defer rollback()
	opts := swift.LargeObjectOpts{
		Container:   CONTAINER,
		ObjectName:  OBJECT,
		Flags:       os.O_APPEND,
		CheckHash:   true,
		ContentType: "image/jpeg",
	}
	out, err := c.StaticLargeObjectCreateFile(ctx, &opts)
	if err != nil {
		t.Fatal(err)
	}

	contents, err := c.ObjectGetString(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
	buf := bytes.NewBuffer([]byte(contents))
	multi := io.MultiWriter(buf, out)
	for i := 0; i < 2; i++ {
		_, err = fmt.Fprintf(multi, "%d %s\n", i+10, CONTENTS)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = out.CloseWithContext(ctx)
	if err != nil {
		t.Error(err)
	}
	expected := buf.String()
	contents, err = c.ObjectGetString(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error(err)
	}
	if contents != expected {
		t.Errorf("Contents wrong, expected %q, got: %q", expected, contents)
	}
}

func TestSLOMove(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithSLO(t)
	defer rollback()
	contents, err := c.ObjectGetString(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}

	err = c.StaticLargeObjectMove(ctx, CONTAINER, OBJECT, CONTAINER, OBJECT2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = c.StaticLargeObjectDelete(ctx, CONTAINER, OBJECT2)
		if err != nil {
			t.Fatal(err)
		}
	}()

	contents2, err := c.ObjectGetString(ctx, CONTAINER, OBJECT2)
	if err != nil {
		t.Fatal(err)
	}

	if contents2 != contents {
		t.Error("Contents wrong")
	}
}

func TestSLONoSegmentContainer(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithSLO(t)
	defer rollback()

	opts := swift.LargeObjectOpts{
		Container:        CONTAINER,
		ObjectName:       OBJECT,
		ContentType:      "image/jpeg",
		SegmentContainer: CONTAINER,
	}
	out, err := c.StaticLargeObjectCreate(ctx, &opts)
	if err != nil {
		t.Fatal(err)
	}

	buf := &bytes.Buffer{}
	multi := io.MultiWriter(buf, out)
	for i := 0; i < 2; i++ {
		_, err = fmt.Fprintf(multi, "%d %s\n", i, CONTENTS)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = out.CloseWithContext(ctx)
	if err != nil {
		t.Error(err)
	}
	expected := buf.String()
	contents, err := c.ObjectGetString(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Error(err)
	}
	if contents != expected {
		t.Errorf("Contents wrong, expected %q, got: %q", expected, contents)
	}

	err = c.StaticLargeObjectDelete(ctx, CONTAINER, OBJECT)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSLOMinChunkSize(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithSegmentsContainer(t)
	defer rollback()
	if srv == nil {
		t.Skipf("This test only runs with the fake swift server as it's needed to simulate min segment size.")
		return
	}

	srv.SetOverride("/info", func(w http.ResponseWriter, r *http.Request, recorder *httptest.ResponseRecorder) {
		_, _ = w.Write([]byte(`{"slo": {"min_segment_size": 4}}`))
	})
	defer srv.UnsetOverride("/info")
	_, _ = c.QueryInfo(ctx)

	opts := swift.LargeObjectOpts{
		Container:    CONTAINER,
		ObjectName:   OBJECT,
		ContentType:  "image/jpeg",
		ChunkSize:    6,
		MinChunkSize: 0,
		NoBuffer:     true,
	}

	testSLOSegmentation(t, c, func() swift.LargeObjectFile {
		out, err := c.StaticLargeObjectCreate(ctx, &opts)
		if err != nil {
			t.Fatal(err)
		}
		return out
	})
}

func TestSLOSegmentation(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithSegmentsContainer(t)
	defer rollback()
	opts := swift.LargeObjectOpts{
		Container:    CONTAINER,
		ObjectName:   OBJECT,
		ContentType:  "image/jpeg",
		ChunkSize:    6,
		MinChunkSize: 4,
		NoBuffer:     true,
	}
	testSLOSegmentation(t, c, func() swift.LargeObjectFile {
		out, err := c.StaticLargeObjectCreate(ctx, &opts)
		if err != nil {
			if err == swift.SLONotSupported {
				t.Skip("SLO not supported")
			}
			t.Fatal(err)
		}
		return out
	})
}

func TestSLOSegmentationBuffered(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithSegmentsContainer(t)
	defer rollback()
	opts := swift.LargeObjectOpts{
		Container:    CONTAINER,
		ObjectName:   OBJECT,
		ContentType:  "image/jpeg",
		ChunkSize:    6,
		MinChunkSize: 4,
	}
	testSegmentation(t, c, func() swift.LargeObjectFile {
		out, err := c.StaticLargeObjectCreate(ctx, &opts)
		if err != nil {
			if err == swift.SLONotSupported {
				t.Skip("SLO not supported")
			}
			t.Fatal(err)
		}
		return out
	}, []segmentTest{
		{
			writes:        []string{"0", "1", "2", "3", "4", "5", "6", "7", "8"},
			expectedSegs:  []string{"012345", "678"},
			expectedValue: "012345678",
		},
		{
			writes:        []string{"012345", "012345"},
			expectedSegs:  []string{"012345", "012345"},
			expectedValue: "012345012345",
		},
		{
			writes:        []string{"0123456", "0123456"},
			expectedSegs:  []string{"012345", "601234", "56"},
			expectedValue: "01234560123456",
		},
		{
			writes:        []string{"0123456", "0123456"},
			seeks:         []int{-4, 0},
			expectedSegs:  []string{"012012", "3456"},
			expectedValue: "0120123456",
		},
		{
			writes:        []string{"0123456", "0123456", "abcde"},
			seeks:         []int{0, -11, 0},
			expectedSegs:  []string{"012abc", "de1234", "56"},
			expectedValue: "012abcde123456",
		},
		{
			writes:        []string{"0123456", "ab"},
			seeks:         []int{-4, 0},
			expectedSegs:  []string{"012ab5", "6"},
			expectedValue: "012ab56",
		},
	})
}

func testSLOSegmentation(t *testing.T, c *swift.Connection, createObj func() swift.LargeObjectFile) {
	testCases := []segmentTest{
		{
			writes:        []string{"0", "1", "2", "3", "4", "5", "6", "7", "8"},
			expectedSegs:  []string{"0123", "4567", "8"},
			expectedValue: "012345678",
		},
		{
			writes:        []string{"012345", "012345"},
			expectedSegs:  []string{"012345", "012345"},
			expectedValue: "012345012345",
		},
		{
			writes:        []string{"0123456", "0123456"},
			expectedSegs:  []string{"012345", "601234", "56"},
			expectedValue: "01234560123456",
		},
		{
			writes:        []string{"0123456", "0123456"},
			seeks:         []int{-4, 0},
			expectedSegs:  []string{"012012", "3456"},
			expectedValue: "0120123456",
		},
		{
			writes:        []string{"0123456", "0123456", "abcde"},
			seeks:         []int{0, -11, 0},
			expectedSegs:  []string{"012abc", "de1234", "56"},
			expectedValue: "012abcde123456",
		},
		{
			writes:        []string{"0123456", "ab"},
			seeks:         []int{-4, 0},
			expectedSegs:  []string{"012ab5", "6"},
			expectedValue: "012ab56",
		},
	}
	testSegmentation(t, c, createObj, testCases)
}

type segmentTest struct {
	writes        []string
	seeks         []int
	expectedSegs  []string
	expectedValue string
}

func testSegmentation(t *testing.T, c *swift.Connection, createObj func() swift.LargeObjectFile, testCases []segmentTest) {
	ctx := context.Background()
	var err error
	runTestCase := func(tCase segmentTest) {
		out := createObj()
		defer func() {
			err = c.LargeObjectDelete(ctx, CONTAINER, OBJECT)
			if err != nil {
				t.Fatal(err)
			}
		}()
		for i, data := range tCase.writes {
			_, err = fmt.Fprint(out, data)
			if err != nil {
				t.Error(err)
			}
			if i < len(tCase.seeks)-1 {
				_, err = out.Seek(int64(tCase.seeks[i]), io.SeekCurrent)
				if err != nil {
					t.Error(err)
				}
			}
		}
		err = out.CloseWithContext(ctx)
		if err != nil {
			t.Error(err)
		}
		contents, err := c.ObjectGetString(ctx, CONTAINER, OBJECT)
		if err != nil {
			t.Error(err)
		}
		if contents != tCase.expectedValue {
			t.Errorf("Contents wrong, expected %q, got: %q", tCase.expectedValue, contents)
		}
		container, objects, err := c.LargeObjectGetSegments(ctx, CONTAINER, OBJECT)
		if err != nil {
			t.Error(err)
		}
		if container != SEGMENTS_CONTAINER {
			t.Errorf("Segments container wrong, expected %q, got: %q", SEGMENTS_CONTAINER, container)
		}
		_, headers, err := c.Object(ctx, CONTAINER, OBJECT)
		if err != nil {
			t.Fatal(err)
		}
		if headers.IsLargeObjectSLO() {
			var info swift.SwiftInfo
			info, err = c.QueryInfo(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if info.SLOMinSegmentSize() > 4 {
				t.Log("Skipping checking segments because SLO min segment size imposed by server is larger than wanted for tests.")
				return
			}
		}
		var segContents []string
		for _, obj := range objects {
			var value string
			value, err = c.ObjectGetString(ctx, SEGMENTS_CONTAINER, obj.Name)
			if err != nil {
				t.Error(err)
			}
			segContents = append(segContents, value)
		}
		if !reflect.DeepEqual(segContents, tCase.expectedSegs) {
			t.Errorf("Segments wrong, expected %#v, got: %#v", tCase.expectedSegs, segContents)
		}
	}
	for _, tCase := range testCases {
		runTestCase(tCase)
	}
}

func TestContainerDelete(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionWithContainer(t)
	defer rollback()
	err := c.ContainerDelete(ctx, CONTAINER)
	if err != nil {
		t.Fatal(err)
	}
	err = c.ContainerDelete(ctx, CONTAINER)
	if !errors.Is(err, swift.ContainerNotFound) {
		t.Fatal("Expecting container not found", err)
	}
	_, _, err = c.Container(ctx, CONTAINER)
	if !errors.Is(err, swift.ContainerNotFound) {
		t.Fatal("Expecting container not found", err)
	}
}

func TestUnAuthenticate(t *testing.T) {
	ctx := context.Background()
	c, rollback := makeConnectionAuth(t)
	defer rollback()
	c.UnAuthenticate()
	if c.Authenticated() {
		t.Fatal("Shouldn't be authenticated")
	}
	// Test re-authenticate
	err := c.Authenticate(ctx)
	if err != nil {
		t.Fatal("ReAuth failed", err)
	}
	if !c.Authenticated() {
		t.Fatal("Not authenticated")
	}
}
