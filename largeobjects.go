package swift

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	gopath "path"
	"strings"
)

// NotLargeObject is returned if an operation is performed on an object which isn't large.
var NotLargeObject = errors.New("Not a large object")

// LargeObjectCreateFile represents an open static or dynamic large object
type LargeObjectCreateFile struct {
	conn          *Connection
	container     string
	objectName    string
	currentLength int64
	filePos       int64
	chunkSize     int64
	prefix        string
	contentType   string
	checkHash     bool
	segments      []Object
	headers       Headers
}

func max(a int64, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func swiftSegmentPath(path string) (string, error) {
	checksum := sha1.New()
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	path = hex.EncodeToString(checksum.Sum(append([]byte(path), random...)))
	return strings.TrimLeft(strings.TrimRight("segments/"+path[0:3]+"/"+path[3:], "/"), "/"), nil
}

func getSegment(segmentPath string, partNumber int) string {
	return fmt.Sprintf("%s/%016d", segmentPath, partNumber)
}

func parseFullPath(manifest string) (container string, prefix string) {
	components := strings.SplitN(manifest, "/", 2)
	container = components[0]
	if len(components) > 1 {
		prefix = components[1]
	}
	return container, prefix
}

func isManifest(headers Headers) bool {
	_, isDLO := headers["X-Object-Manifest"]
	_, isSLO := headers["X-Static-Large-Object"]
	return isSLO || isDLO
}

func (c *Connection) getAllSegments(container string, path string, headers Headers) (segments []Object, err error) {
	if manifest, isDLO := headers["X-Object-Manifest"]; isDLO {
		container, segmentPath := parseFullPath(manifest)
		segments, err = c.ObjectsAll(container, &ObjectsOpts{Prefix: segmentPath})
		if err != nil {
			return nil, nil
		}
	} else if _, isSLO := headers["X-Static-Large-Object"]; isSLO {
		var segmentList []swiftSegment

		values := url.Values{}
		values.Set("multipart-manifest", "get")

		file, _, err := c.objectOpen(container, path, true, nil, values)
		if err != nil {
			return nil, err
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, err
		}

		json.Unmarshal(content, &segmentList)
		for _, segment := range segmentList {
			_, segPath := parseFullPath(segment.Name[1:])
			segments = append(segments, Object{
				Name:  segPath,
				Bytes: segment.Bytes,
				Hash:  segment.Hash,
			})
		}
	}
	return segments, nil
}

// LargeObjectCreate creates a large object at container, objectName.
//
// flags can have the following bits set
//   os.TRUNC  - remove the contents of the large object if it exists
//   os.APPEND - write at the end of the large object
func (c *Connection) LargeObjectCreate(container string, objectName string, flags int, checkHash bool, Hash string, contentType string, h Headers) (*LargeObjectCreateFile, error) {
	var (
		segmentPath   string
		segments      []Object
		currentLength int64
	)

	info, headers, err := c.Object(container, objectName)

	if err == nil {
		if isManifest(headers) {
			segments, err = c.getAllSegments(container, objectName, headers)
			if err != nil {
				return nil, err
			}
			if len(segments) > 0 {
				segmentPath = gopath.Dir(segments[0].Name)
			}
		} else {
			if segmentPath, err = swiftSegmentPath(objectName); err != nil {
				return nil, err
			}
			if err := c.ObjectMove(container, objectName, container, getSegment(segmentPath, 1)); err != nil {
				return nil, err
			}
			segments = append(segments, info)
		}
		if flags&os.O_TRUNC != 0 {
			c.LargeObjectDelete(container, objectName)
		}
		currentLength = info.Bytes
	} else if err == ObjectNotFound {
		if segmentPath, err = swiftSegmentPath(objectName); err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}

	file := &LargeObjectCreateFile{
		conn:          c,
		checkHash:     checkHash,
		container:     container,
		objectName:    objectName,
		chunkSize:     10 * 1024 * 1024,
		prefix:        segmentPath,
		segments:      segments,
		currentLength: currentLength,
	}

	if flags&os.O_APPEND != 0 {
		file.filePos = currentLength
	}

	return file, nil
}

// LargeObjectDelete deletes the large object named by container, path
func (c *Connection) LargeObjectDelete(container string, path string) error {
	info, headers, err := c.Object(container, path)
	if err != nil {
		return err
	}

	var objects []Object
	if isManifest(headers) {
		segments, err := c.getAllSegments(container, info.Name, headers)
		if err != nil {
			return err
		}
		objects = append(objects, segments...)
	}
	objects = append(objects, info)

	if false && len(objects) > 0 {
		filenames := make([]string, len(objects))
		for i, obj := range objects {
			filenames[i] = obj.Name
		}
		_, err = c.BulkDelete(container, filenames)
		// Don't fail on ObjectNotFound because eventual consistency
		// makes this situation normal.
		if err != nil && err != Forbidden && err != ObjectNotFound {
			return err
		}
	} else {
		for _, obj := range objects {
			if err := c.ObjectDelete(container, obj.Name); err != nil {
				return err
			}
		}
	}

	return nil
}

// LargeObjectGetSegments returns all the segments that compose an object
// If the object is a Dynamic Large Object (DLO), it just returns the objects
// that have the prefix as indicated by the manifest.
// If the object is a Static Large Object (SLO), it retrieves the JSON content
// of the manifest and return all the segments of it.
func (c *Connection) LargeObjectGetSegments(container string, path string) (segments []Object, err error) {
	_, headers, err := c.Object(container, path)
	if err != nil {
		return nil, err
	}

	if manifest, isDLO := headers["X-Object-Manifest"]; isDLO {
		container, segmentPath := parseFullPath(manifest)
		return c.ObjectsAll(container, &ObjectsOpts{Prefix: segmentPath})
	}

	if _, isSLO := headers["X-Static-Large-Object"]; isSLO {
		var buf bytes.Buffer
		var segmentList []swiftSegment
		headers := make(Headers)
		headers["X-Static-Large-Object"] = "True"
		if _, err := c.ObjectGet(container, path, &buf, true, headers); err != nil {
			return nil, err
		}
		json.Unmarshal(buf.Bytes(), &segmentList)
		for _, segment := range segmentList {
			_, segPath := parseFullPath(segment.Name[1:])
			segments = append(segments, Object{
				Name:  segPath,
				Bytes: segment.Bytes,
				Hash:  segment.Hash,
			})
		}
		return segments, nil
	}

	return nil, NotLargeObject
}

// Seek sets the offset for the next write operation
func (file *LargeObjectCreateFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		file.filePos = offset
	case 1:
		file.filePos += offset
	case 2:
		file.filePos = file.currentLength + offset
	default:
		return -1, fmt.Errorf("invalid value for whence")
	}
	if file.filePos < 0 {
		return -1, fmt.Errorf("negative offset")
	}
	return file.filePos, nil
}
