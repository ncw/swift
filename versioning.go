package swift

import (
	"fmt"
)

// VersionContainerCreate is a helper method for creating and enabling version controlled containers.
//
// It builds the current object container, the non-current object version container, and enables versioning.
func (c *Connection) VersionContainerCreate(current, version string) error {
	if err := c.ContainerCreate(version, nil); err != nil {
		return err
	}
	if err := c.ContainerCreate(current, nil); err != nil {
		return err
	}
	if err := c.VersionEnable(current, version); err != nil {
		return err
	}
	return nil
}

// VersionEnable enables versioning on the current container with version as the tracking container.
func (c *Connection) VersionEnable(current, version string) error {
	h := Headers{"X-Versions-Location": version}
	if err := c.ContainerUpdate(current, h); err != nil {
		return err
	}
	return nil
}

// VersionDisable disables versioning on the current container.
func (c *Connection) VersionDisable(current string) error {
	h := Headers{"X-Versions-Location": ""}
	if err := c.ContainerUpdate(current, h); err != nil {
		return err
	}
	return nil
}

// VersionObjectList returns a list of older versions of the object.
//
// Objects are returned in the format <length><object_name>/<timestamp>
func (c *Connection) VersionObjectList(version, object string) ([]string, error) {
	opts := &ObjectsOpts{
		// <3-character zero-padded hexadecimal character length><object name>/
		Prefix: fmt.Sprintf("%03x", len(object)) + object + "/",
	}
	return c.ObjectNames(version, opts)
}
