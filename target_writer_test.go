package main

import (
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tardigodev/tardigo-core/pkg"
	"github.com/tardigodev/tardigo-core/pkg/constants"
	"github.com/tardigodev/tardigo-core/pkg/objects"
)

func TestTargetWriterPlugin(t *testing.T) {
	var _ pkg.TargetWriterPlugin = TargetWriterPlugin

	assert.Equal(t, 2, reflect.TypeOf(TargetWriterPlugin).NumField())
	assert.Equal(t, "", TargetWriterPlugin.DirPath)
	assert.Equal(t, "", TargetWriterPlugin.FileSuffix)
}

func TestTargetWriterPlugin_PutRecord(t *testing.T) {
	twp := targetWriterPlugin{
		DirPath:    ".temp/",
		FileSuffix: ".xyz",
	}
	defer os.RemoveAll(".temp/")
	recordDetails := make([]objects.WriterDetail, 0)

	err := twp.GetWriter(func(w io.Writer, wd objects.WriterDetail) error {
		recordDetails = append(recordDetails, wd)
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, len(recordDetails))
	assert.Equal(t, constants.StreamTypeOk, recordDetails[0].WriterType)

	assert.FileExists(t, recordDetails[0].WriterTarget)

	// invalid dirPath
	twp = targetWriterPlugin{
		DirPath:    "/invalid/",
		FileSuffix: ".xyz",
	}

	recordDetails = make([]objects.WriterDetail, 0)

	err = twp.GetWriter(func(w io.Writer, wd objects.WriterDetail) error {
		recordDetails = append(recordDetails, wd)
		return nil
	})
	assert.NoError(t, err)

	assert.Equal(t, 1, len(recordDetails))
	assert.Equal(t, constants.StreamTypeFailed, recordDetails[0].WriterType)

}

func TestTargetWriterPlugin_GetPluginDetail(t *testing.T) {
	detail := targetWriterPlugin{}.GetPluginDetail()
	assert.Equal(t, "local_file_storage_writer", detail.PluginName)
	assert.Equal(t, constants.PluginTypeTargetWriter, detail.PluginType)
}
