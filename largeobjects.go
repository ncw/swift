package swift

import (
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
	conn             *Connection
	container        string
	objectName       string
	currentLength    int64
	filePos          int64
	chunkSize        int64
	segmentContainer string
	prefix           string
	contentType      string
	checkHash        bool
	segments         []Object
	headers          Headers
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

func (c *Connection) getAllSegments(container string, path string, headers Headers) (string, []Object, error) {
	if manifest, isDLO := headers["X-Object-Manifest"]; isDLO {
		segmentContainer, segmentPath := parseFullPath(manifest)
		segments, err := c.ObjectsAll(segmentContainer, &ObjectsOpts{Prefix: segmentPath})
		return segmentContainer, segments, err
	}

	if _, isSLO := headers["X-Static-Large-Object"]; isSLO {
		var (
			segmentList      []swiftSegment
			segments         []Object
			segPath          string
			segmentContainer string
		)

		values := url.Values{}
		values.Set("multipart-manifest", "get")

		file, _, err := c.objectOpen(container, path, true, nil, values)
		if err != nil {
			return "", nil, err
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			return "", nil, err
		}

		json.Unmarshal(content, &segmentList)
		for _, segment := range segmentList {
			segmentContainer, segPath = parseFullPath(segment.Name[1:])
			segments = append(segments, Object{
				Name:  segPath,
				Bytes: segment.Bytes,
				Hash:  segment.Hash,
			})
		}

		return segmentContainer, segments, nil
	}

	return "", nil, NotLargeObject
}

// LargeObjectOpts describes how a large object should be created
type LargeObjectOpts struct {
	Container        string  // Name of container to place object
	ObjectName       string  // Name of object
	Flags            int     // Creation flags
	CheckHash        bool    // If set Check the hash
	Hash             string  // If set use this hash to check
	ContentType      string  // Content-Type of the object
	Headers          Headers // Additional headers to upload the object with
	ChunkSize        int64   // Size of chunks of the object, defaults to 10MB if not set
	SegmentContainer string  // Name of the container to place segments
	SegmentPrefix    string  // Prefix to use for the segments
}

// LargeObjectCreate creates a large object at opts.Container, opts.ObjectName.
//
// opts.Flags can have the following bits set
//   os.TRUNC  - remove the contents of the large object if it exists
//   os.APPEND - write at the end of the large object
func (c *Connection) LargeObjectCreate(opts *LargeObjectOpts) (*LargeObjectCreateFile, error) {
	var (
		segmentPath      string
		segmentContainer string
		segments         []Object
		currentLength    int64
		err              error
	)

	if opts.SegmentPrefix != "" {
		segmentPath = opts.SegmentPrefix
	} else if segmentPath, err = swiftSegmentPath(opts.ObjectName); err != nil {
		return nil, err
	}

	if info, headers, err := c.Object(opts.Container, opts.ObjectName); err == nil {
		if isManifest(headers) {
			segmentContainer, segments, err = c.getAllSegments(opts.Container, opts.ObjectName, headers)
			if err != nil {
				return nil, err
			}
			if len(segments) > 0 {
				segmentPath = gopath.Dir(segments[0].Name)
			}
		} else {
			if err := c.ObjectMove(opts.Container, opts.ObjectName, opts.Container, getSegment(segmentPath, 1)); err != nil {
				return nil, err
			}
			segments = append(segments, info)
		}
		if opts.Flags&os.O_TRUNC != 0 {
			c.LargeObjectDelete(opts.Container, opts.ObjectName)
		} else {
			currentLength = info.Bytes
		}
	} else if err != ObjectNotFound {
		return nil, err
	}

	// segmentContainer is not empty when the manifest already existed
	if segmentContainer == "" {
		if opts.SegmentContainer != "" {
			segmentContainer = opts.SegmentContainer
		} else {
			segmentContainer = opts.Container + "_segments"
		}
	}

	file := &LargeObjectCreateFile{
		conn:             c,
		checkHash:        opts.CheckHash,
		container:        opts.Container,
		objectName:       opts.ObjectName,
		chunkSize:        opts.ChunkSize,
		segmentContainer: segmentContainer,
		prefix:           segmentPath,
		segments:         segments,
		currentLength:    currentLength,
	}

	if file.chunkSize == 0 {
		file.chunkSize = 10 * 1024 * 1024
	}

	if opts.Flags&os.O_APPEND != 0 {
		file.filePos = currentLength
	}

	return file, nil
}

// LargeObjectDelete deletes the large object named by container, path
func (c *Connection) LargeObjectDelete(container string, objectName string) error {
	_, headers, err := c.Object(container, objectName)
	if err != nil {
		return err
	}

	var objects [][]string
	if isManifest(headers) {
		segmentContainer, segments, err := c.getAllSegments(container, objectName, headers)
		if err != nil {
			return err
		}
		for _, obj := range segments {
			objects = append(objects, []string{segmentContainer, obj.Name})
		}
	}
	objects = append(objects, []string{container, objectName})

	info, err := c.cachedQueryInfo()
	if err != nil {
		return err
	}

	if info.SupportsBulkDelete() && len(objects) > 0 {
		filenames := make([]string, len(objects))
		for i, obj := range objects {
			filenames[i] = obj[0] + "/" + obj[1]
		}
		_, err = c.doBulkDelete(filenames)
		// Don't fail on ObjectNotFound because eventual consistency
		// makes this situation normal.
		if err != nil && err != Forbidden && err != ObjectNotFound {
			return err
		}
	} else {
		for _, obj := range objects {
			if err := c.ObjectDelete(obj[0], obj[1]); err != nil {
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
func (c *Connection) LargeObjectGetSegments(container string, path string) (string, []Object, error) {
	_, headers, err := c.Object(container, path)
	if err != nil {
		return "", nil, err
	}

	return c.getAllSegments(container, path, headers)
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
