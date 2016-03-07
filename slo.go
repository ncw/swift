package swift

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"time"
)

// StaticLargeObjectCreateFile represents an open static large object
type StaticLargeObjectCreateFile struct {
	LargeObjectCreateFile
}

// minChunkSize defines the minimum size of a segment
const minChunkSize = 1 << 20

// readAfterWriteTimeout defines the time we wait before an object appears after having been uploaded
var readAfterWriteTimeout = 15 * time.Second

// readAfterWriteWait defines the time to sleep between two retries
var readAfterWriteWait = 200 * time.Millisecond

type swiftSegment struct {
	Path string `json:"path,omitempty"`
	Hash string `json:"hash,omitempty"`
	Size int64  `json:"size_bytes,omitempty"`
	// When uploading a manifest, the attributes must be named `path`, `hash` and `size_bytes`
	// but when querying the JSON content of a manifest with the `multipart-manifest=get`
	// parameter, Swift names those attributes `name`, `etag` and `bytes`.
	// We use all the different attributes names in this structure to be able to use
	// the same structure for both uploading and retrieving.
	Name         string `json:"name,omitempty"`
	Etag         string `json:"etag,omitempty"`
	Bytes        int64  `json:"bytes,omitempty"`
	ContentType  string `json:"content_type,omitempty"`
	LastModified string `json:"last_modified,omitempty"`
}

// Check it satisfies the interfaces
var (
	_ io.Writer     = &StaticLargeObjectCreateFile{}
	_ io.Seeker     = &StaticLargeObjectCreateFile{}
	_ io.Closer     = &StaticLargeObjectCreateFile{}
	_ io.ReaderFrom = &StaticLargeObjectCreateFile{}
)

// StaticLargeObjectCreateFile creates a static large object returning
// an object which satisfies io.Writer, io.Seeker, io.Closer and
// io.ReaderFrom.  The flags are as passes to the LargeObjectCreate
// method.
func (c *Connection) StaticLargeObjectCreateFile(container string, objectName string, flags int, checkHash bool, Hash string, contentType string, h Headers) (*StaticLargeObjectCreateFile, error) {
	lo, err := c.LargeObjectCreate(container, objectName, flags, checkHash, Hash, contentType, h)
	if err != nil {
		return nil, err
	}

	return &StaticLargeObjectCreateFile{
		LargeObjectCreateFile: *lo,
	}, nil
}

// StaticLargeObjectCreate creates or truncates an existing static
// large object returning a writeable object.
func (c *Connection) StaticLargeObjectCreate(container string, objectName string, checkHash bool, Hash string, contentType string, h Headers) (*StaticLargeObjectCreateFile, error) {
	return c.StaticLargeObjectCreateFile(container, objectName, os.O_TRUNC|os.O_CREATE, checkHash, Hash, contentType, h)
}

// StaticLargeObjectDelete deletes a static large object and all of its segments.
func (c *Connection) StaticLargeObjectDelete(container string, path string) error {
	return c.LargeObjectDelete(container, path)
}

// StaticLargeObjectMove moves a static large object from srcContainer, srcObjectName to dstContainer, dstObjectName
func (c *Connection) StaticLargeObjectMove(srcContainer string, srcObjectName string, dstContainer string, dstObjectName string) error {
	info, headers, err := c.Object(srcContainer, srcObjectName)
	if err != nil {
		return err
	}

	segments, err := c.getAllSegments(srcContainer, srcObjectName, headers)
	if err != nil {
		return err
	}

	if err := c.createSLOManifest(dstContainer, dstObjectName, info.ContentType, segments); err != nil {
		return err
	}

	if err := c.ObjectDelete(srcContainer, srcObjectName); err != nil {
		return err
	}

	return nil
}

// createSLOManifest creates a static large object manifest
func (c *Connection) createSLOManifest(container string, path string, contentType string, segments []Object) error {
	sloSegments := make([]swiftSegment, len(segments))
	for i, segment := range segments {
		sloSegments[i].Path = fmt.Sprintf("%s/%s", container, segment.Name)
		sloSegments[i].Etag = segment.Hash
		sloSegments[i].Size = segment.Bytes
	}

	content, err := json.Marshal(sloSegments)
	if err != nil {
		return err
	}

	values := url.Values{}
	values.Set("multipart-manifest", "put")
	if _, err := c.objectPut(container, path, bytes.NewBuffer(content), false, "", contentType, nil, values); err != nil {
		return err
	}

	return nil
}

// Write satisfies the io.Writer interface
func (file *StaticLargeObjectCreateFile) Write(buf []byte) (int, error) {
	reader := bytes.NewReader(buf)
	n, err := file.ReadFrom(reader)
	return int(n), err
}

// ReadFrom statisfies the io.ReaderFrom interface
func (file *StaticLargeObjectCreateFile) ReadFrom(reader io.Reader) (n int64, err error) {
	var (
		multi         io.Reader
		paddingReader io.Reader
		currentLength int64
		cursor        int64
	)

	partNumber := 1
	totalRead := int64(0)
	chunkSize := int64(file.chunkSize)
	zeroBuf := make([]byte, file.chunkSize)
	readers := []io.Reader{}
	hash := md5.New()

	for _, segment := range file.segments {
		currentLength += segment.Bytes
	}

	// First, we skip the existing segments that are not modified by this call
	for i := range file.segments {
		if file.filePos < cursor+file.segments[i].Bytes || (file.segments[i].Bytes < minChunkSize) {
			break
		}
		cursor += file.segments[i].Bytes
		partNumber++
		hash.Write([]byte(file.segments[i].Hash))
	}

	if file.filePos-cursor > 0 && min(currentLength, file.filePos)-cursor > 0 {
		// Offset is inside the current segment : we need to read the
		// data from the beginning of the segment to offset
		headers := make(Headers)
		headers["Range"] = "bytes=" + strconv.FormatInt(cursor, 10) + "-" + strconv.FormatInt(min(currentLength, file.filePos)-1, 10)
		f, _, err := file.conn.ObjectOpen(file.container, file.objectName, false, headers)
		if err != nil {
			return 0, err
		}
		defer f.Close()
		paddingReader = f
		readers = append(readers, io.LimitReader(paddingReader, min(currentLength, file.filePos)-cursor))
		totalRead -= min(currentLength, file.filePos) - cursor
	}

	// We reached the end of the file but we haven't reached 'offset' yet
	// Therefore we add blocks of zeros
	for zeros := file.filePos - currentLength; zeros > 0; zeros -= chunkSize {
		readers = append(readers, io.LimitReader(bytes.NewReader(zeroBuf), min(chunkSize, zeros)))
	}

	totalRead -= max(0, file.filePos-currentLength)
	readers = append(readers, reader)
	multi = io.MultiReader(readers...)

	writeSegment := func(segment string) (finished bool, bytesRead int64, err error) {
		currentSegment, err := file.conn.ObjectCreate(file.container, segment, false, "", file.contentType, nil)
		if err != nil {
			return false, bytesRead, err
		}

		segmentHash := md5.New()
		writer := io.MultiWriter(currentSegment, segmentHash)

		n, err := io.Copy(writer, io.LimitReader(multi, chunkSize))
		if err != nil {
			return false, bytesRead, err
		}

		if n > 0 {
			defer func() {
				closeError := currentSegment.Close()
				if err != nil {
					err = closeError
				}

				hexHash := hex.EncodeToString(segmentHash.Sum(nil))
				hash.Write([]byte(hexHash))
				infos, _, _ := file.conn.Object(file.container, segment)
				if partNumber > len(file.segments) {
					file.segments = append(file.segments, Object{
						Name: segment,
					})
				}
				file.segments[partNumber-1].Bytes = infos.Bytes
				file.segments[partNumber-1].Hash = hexHash
			}()
			bytesRead = n
		}

		if n < chunkSize {
			end := currentLength
			if partNumber-1 < len(file.segments) {
				end = cursor + file.segments[partNumber-1].Bytes
			}
			if cursor+n < end {
				// Copy the end of the chunk
				headers := make(Headers)
				headers["Range"] = "bytes=" + strconv.FormatInt(cursor+n, 10) + "-" + strconv.FormatInt(cursor+chunkSize-1, 10)
				file, _, err := file.conn.ObjectOpen(file.container, file.objectName, false, headers)
				if err != nil {
					return false, bytesRead, err
				}

				_, copyErr := io.Copy(writer, file)

				if err := file.Close(); err != nil {
					return false, bytesRead, err
				}

				if copyErr != nil {
					return false, bytesRead, copyErr
				}
			}

			return true, bytesRead, nil
		}

		cursor += chunkSize
		return false, bytesRead, nil
	}

	err = nil
	finished := false
	read := int64(0)
	bytesRead := int64(0)
	for ; finished == false; partNumber++ {
		finished, read, err = writeSegment(getSegment(file.prefix, partNumber))
		totalRead += read
		bytesRead = max(0, totalRead)
		if err != nil {
			return bytesRead, err
		}
	}

	file.filePos += totalRead

	for ; partNumber <= len(file.segments); partNumber++ {
		hash.Write([]byte(file.segments[partNumber-1].Hash))
	}

	waitingTime := readAfterWriteWait
	endTime := time.Now().Add(readAfterWriteTimeout)
	for {
		if err = file.conn.createSLOManifest(file.container, file.objectName, file.contentType, file.segments); err == nil {
			break
		}
		if time.Now().Add(waitingTime).After(endTime) {
			return 0, err
		}
		time.Sleep(waitingTime)
		waitingTime *= 2
	}

	return bytesRead, err
}

// Close satisfies the io.Closer interface
func (file *StaticLargeObjectCreateFile) Close() error {
	if err := file.conn.createSLOManifest(file.container, file.objectName, file.contentType, file.segments); err != nil {
		return err
	}

	return nil
}
