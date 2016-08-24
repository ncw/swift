package swift

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"strconv"
)

// StaticLargeObjectCreateFile represents an open static large object
type StaticLargeObjectCreateFile struct {
	LargeObjectCreateFile
}

// minChunkSize defines the minimum size of a segment
const minChunkSize = 1 << 20

var SLONotSupported = errors.New("SLO not supported")

type swiftSegment struct {
	Path string `json:"path,omitempty"`
	Etag string `json:"etag,omitempty"`
	Size int64  `json:"size_bytes,omitempty"`
	// When uploading a manifest, the attributes must be named `path`, `etag` and `size_bytes`
	// but when querying the JSON content of a manifest with the `multipart-manifest=get`
	// parameter, Swift names those attributes `name`, `hash` and `bytes`.
	// We use all the different attributes names in this structure to be able to use
	// the same structure for both uploading and retrieving.
	Name         string `json:"name,omitempty"`
	Hash         string `json:"hash,omitempty"`
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
// io.ReaderFrom.  The flags are as passed to the LargeObjectCreate
// method.
func (c *Connection) StaticLargeObjectCreateFile(opts *LargeObjectOpts) (*StaticLargeObjectCreateFile, error) {
	info, err := c.cachedQueryInfo()
	if err != nil {
		return nil, err
	}
	if !info.SupportsSLO() {
		return nil, SLONotSupported
	}
	lo, err := c.LargeObjectCreate(opts)
	if err != nil {
		return nil, err
	}

	return &StaticLargeObjectCreateFile{
		LargeObjectCreateFile: *lo,
	}, nil
}

// StaticLargeObjectCreate creates or truncates an existing static
// large object returning a writeable object. This sets opts.Flags to
// an appropriate value before calling StaticLargeObjectCreateFile
func (c *Connection) StaticLargeObjectCreate(opts *LargeObjectOpts) (*StaticLargeObjectCreateFile, error) {
	opts.Flags = os.O_TRUNC | os.O_CREATE
	return c.StaticLargeObjectCreateFile(opts)
}

// StaticLargeObjectDelete deletes a static large object and all of its segments.
func (c *Connection) StaticLargeObjectDelete(container string, path string) error {
	info, err := c.cachedQueryInfo()
	if err != nil {
		return err
	}
	if !info.SupportsSLO() {
		return SLONotSupported
	}
	return c.LargeObjectDelete(container, path)
}

// StaticLargeObjectMove moves a static large object from srcContainer, srcObjectName to dstContainer, dstObjectName
func (c *Connection) StaticLargeObjectMove(srcContainer string, srcObjectName string, dstContainer string, dstObjectName string) error {
	swiftInfo, err := c.cachedQueryInfo()
	if err != nil {
		return err
	}
	if !swiftInfo.SupportsSLO() {
		return SLONotSupported
	}
	info, headers, err := c.Object(srcContainer, srcObjectName)
	if err != nil {
		return err
	}

	container, segments, err := c.getAllSegments(srcContainer, srcObjectName, headers)
	if err != nil {
		return err
	}

	if err := c.createSLOManifest(dstContainer, dstObjectName, info.ContentType, container, segments); err != nil {
		return err
	}

	if err := c.ObjectDelete(srcContainer, srcObjectName); err != nil {
		return err
	}

	return nil
}

// createSLOManifest creates a static large object manifest
func (c *Connection) createSLOManifest(container string, path string, contentType string, segmentContainer string, segments []Object) error {
	sloSegments := make([]swiftSegment, len(segments))
	for i, segment := range segments {
		sloSegments[i].Path = fmt.Sprintf("%s/%s", segmentContainer, segment.Name)
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
		// Offset is inside the current segment : we need to read the data from
		// the beginning of the segment to offset, for this we must ensure that
		// the manifest is already written.
		err = file.Flush()
		if err != nil {
			return 0, err
		}
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
		currentSegment, err := file.conn.ObjectCreate(file.segmentContainer, segment, false, "", file.contentType, nil)
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
				if closeError != nil {
					err = closeError
				}

				hexHash := hex.EncodeToString(segmentHash.Sum(nil))
				hash.Write([]byte(hexHash))
				infos, _, _ := file.conn.Object(file.segmentContainer, segment)
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
				headers["Range"] = "bytes=" + strconv.FormatInt(cursor+n, 10) + "-" + strconv.FormatInt(end-1, 10)
				file, _, err := file.conn.ObjectOpen(file.container, file.objectName, false, headers)
				if err != nil {
					return false, bytesRead, err
				}

				_, copyErr := io.Copy(writer, file)
				if copyErr != nil {
					return false, bytesRead, copyErr
				}

				if err := file.Close(); err != nil {
					return false, bytesRead, err
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

	file.currentLength = max(file.filePos+bytesRead, currentLength)
	file.filePos += totalRead

	for ; partNumber <= len(file.segments); partNumber++ {
		hash.Write([]byte(file.segments[partNumber-1].Hash))
	}

	return bytesRead, err
}

// Close satisfies the io.Closer interface
func (file *StaticLargeObjectCreateFile) Close() error {
	return file.Flush()
}

func (file *StaticLargeObjectCreateFile) Flush() error {
	if err := file.conn.createSLOManifest(file.container, file.objectName, file.contentType, file.segmentContainer, file.segments); err != nil {
		return err
	}
	return file.conn.waitForSegmentsToShowUp(file.container, file.objectName, file.Size())
}

func (c *Connection) getAllSLOSegments(container, path string) (string, []Object, error) {
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
