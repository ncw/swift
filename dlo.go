package swift

import (
	"bytes"
	"crypto/md5"
	"io"
	"os"
	"strconv"
)

// DynamicLargeObjectCreateFile represents an open static large object
type DynamicLargeObjectCreateFile struct {
	LargeObjectCreateFile
}

// Check it satisfies the interfaces
var (
	_ io.Writer     = &DynamicLargeObjectCreateFile{}
	_ io.Seeker     = &DynamicLargeObjectCreateFile{}
	_ io.Closer     = &DynamicLargeObjectCreateFile{}
	_ io.ReaderFrom = &DynamicLargeObjectCreateFile{}
)

// DynamicLargeObjectCreateFile creates a dynamic large object
// returning an object which satisfies io.Writer, io.Seeker, io.Closer
// and io.ReaderFrom.  The flags are as passes to the
// LargeObjectCreate method.
func (c *Connection) DynamicLargeObjectCreateFile(opts *LargeObjectOpts) (*DynamicLargeObjectCreateFile, error) {
	lo, err := c.LargeObjectCreate(opts)
	if err != nil {
		return nil, err
	}

	return &DynamicLargeObjectCreateFile{
		LargeObjectCreateFile: *lo,
	}, nil
}

// DynamicLargeObjectCreate creates or truncates an existing dynamic
// large object returning a writeable object.  This sets opts.Flags to
// an appropriate value before calling DynamicLargeObjectCreateFile
func (c *Connection) DynamicLargeObjectCreate(opts *LargeObjectOpts) (*DynamicLargeObjectCreateFile, error) {
	opts.Flags = os.O_TRUNC | os.O_CREATE
	return c.DynamicLargeObjectCreateFile(opts)
}

// DynamicLargeObjectDelete deletes a dynamic large object and all of its segments.
func (c *Connection) DynamicLargeObjectDelete(container string, path string) error {
	return c.LargeObjectDelete(container, path)
}

// DynamicLargeObjectMove moves a dynamic large object from srcContainer, srcObjectName to dstContainer, dstObjectName
func (c *Connection) DynamicLargeObjectMove(srcContainer string, srcObjectName string, dstContainer string, dstObjectName string) error {
	info, headers, err := c.Object(dstContainer, srcObjectName)
	if err != nil {
		return err
	}

	_, segmentPath := parseFullPath(headers["X-Object-Manifest"])
	if err := c.createDLOManifest(dstContainer, dstObjectName, dstContainer+"/"+segmentPath, info.ContentType); err != nil {
		return err
	}

	if err := c.ObjectDelete(srcContainer, srcObjectName); err != nil {
		return err
	}

	return nil
}

// createDLOManifest creates a dynamic large object manifest
func (c *Connection) createDLOManifest(container string, objectName string, prefix string, contentType string) error {
	headers := make(Headers)
	headers["X-Object-Manifest"] = prefix
	manifest, err := c.ObjectCreate(container, objectName, false, "", contentType, headers)
	if err != nil {
		return err
	}

	if err := manifest.Close(); err != nil {
		return err
	}

	return nil
}

// Write satisfies the io.Writer interface
func (file *DynamicLargeObjectCreateFile) Write(buf []byte) (int, error) {
	reader := bytes.NewReader(buf)
	n, err := file.ReadFrom(reader)
	return int(n), err
}

// ReadFrom statisfies the io.ReaderFrom interface
func (file *DynamicLargeObjectCreateFile) ReadFrom(reader io.Reader) (n int64, err error) {
	var (
		multi         io.Reader
		paddingReader io.Reader
		cursor        int64
		currentLength int64
	)

	partNumber := 1
	totalRead := int64(0)
	chunkSize := int64(file.chunkSize)
	readers := []io.Reader{}
	hash := md5.New()

	file.segments, err = file.conn.ObjectsAll(file.segmentContainer, &ObjectsOpts{Prefix: file.prefix})
	if err != nil {
		return 0, err
	}

	for _, segment := range file.segments {
		currentLength += segment.Bytes
	}

	// First, we skip the existing segments that are not modified by this call
	for i := range file.segments {
		if file.filePos < cursor+file.segments[i].Bytes {
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
		currentSegment, _, err := file.conn.ObjectOpen(file.container, file.objectName, false, headers)
		if err != nil {
			return 0, err
		}
		defer currentSegment.Close()
		paddingReader = currentSegment
		readers = append(readers, io.LimitReader(paddingReader, min(currentLength, file.filePos)-cursor))
		totalRead -= min(currentLength, file.filePos) - cursor
	}

	if paddingReader != nil {
		readers = append(readers, io.LimitReader(paddingReader, file.filePos-cursor))
	}
	readers = append(readers, io.LimitReader(reader, chunkSize-(file.filePos-cursor)))
	multi = io.MultiReader(readers...)

	writeSegment := func() (finished bool, bytesRead int64, err error) {
		segment := getSegment(file.prefix, partNumber)

		currentSegment, err := file.conn.ObjectCreate(file.segmentContainer, segment, false, "", file.contentType, nil)
		if err != nil {
			return false, bytesRead, err
		}

		buf := new(bytes.Buffer)
		multi2 := io.MultiWriter(currentSegment, buf)

		n, err := io.Copy(multi2, multi)
		if err != nil {
			return false, bytesRead, err
		}

		if n > 0 {
			defer currentSegment.Close()
			bytesRead += n - max(0, file.filePos-cursor)
		}

		if n < chunkSize {
			// We wrote all the data
			end := currentLength
			if partNumber-1 < len(file.segments) {
				end = cursor + file.segments[partNumber-1].Bytes
			}
			if cursor+n < end {
				// Copy the end of the chunk
				headers := make(Headers)
				headers["Range"] = "bytes=" + strconv.FormatInt(cursor+n, 10) + "-" + strconv.FormatInt(end-1, 10)
				f, _, err := file.conn.ObjectOpen(file.container, file.objectName, false, headers)
				if err != nil {
					return false, bytesRead, err
				}

				_, copyErr := io.Copy(currentSegment, f)

				if err := f.Close(); err != nil {
					return false, bytesRead, err
				}

				if copyErr != nil {
					return false, bytesRead, copyErr
				}
			}

			return true, bytesRead, nil
		}

		multi = io.LimitReader(reader, chunkSize)
		cursor += chunkSize
		partNumber++

		return false, bytesRead, nil
	}

	finished := false
	read := int64(0)
	bytesRead := int64(0)
	for finished == false {
		finished, read, err = writeSegment()
		bytesRead += read
		file.filePos += read
		if err != nil {
			return bytesRead, err
		}
	}

	return bytesRead, nil
}

// Close satisfies the io.Closer interface
func (file *DynamicLargeObjectCreateFile) Close() error {
	return file.conn.createDLOManifest(file.container, file.objectName, file.segmentContainer+"/"+file.prefix, file.contentType)
}
