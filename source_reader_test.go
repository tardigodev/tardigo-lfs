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

func TestSourceReaderPlugin(t *testing.T) {
	var _ pkg.SourceReaderPlugin = SourceReaderPlugin

	assert.Equal(t, 1, reflect.TypeOf(SourceReaderPlugin).NumField())
	assert.Equal(t, []string{}, SourceReaderPlugin.Paths)

}

func TestSourceReaderPlugin_GetReader(t *testing.T) {
	c, err := os.Create(".test.csv")
	c.Close()
	defer os.RemoveAll(".test.csv")

	assert.NoError(t, err)

	SourceReaderPlugin.Paths = []string{".test.csv", "invalid.csv"}

	readerDetails := []objects.ReaderDetail{}
	err = SourceReaderPlugin.GetReader(func(r io.Reader, rd objects.ReaderDetail) error {
		readerDetails = append(readerDetails, rd)
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 3, len(readerDetails))

	assert.NoError(t, readerDetails[0].ReaderError)
	assert.Equal(t, constants.StreamTypeOk, readerDetails[0].ReaderType)
	assert.Error(t, readerDetails[1].ReaderError)
	assert.Equal(t, constants.StreamTypeFailed, readerDetails[1].ReaderType)
	assert.NoError(t, readerDetails[2].ReaderError)
	assert.Equal(t, constants.StreamTypeEnd, readerDetails[2].ReaderType)

}

func TestSourceReaderPlugin_GetPluginDetail(t *testing.T) {
	detail := sourceReaderPlugin{}.GetPluginDetail()
	assert.Equal(t, "local_file_storage_reader", detail.PluginName)
	assert.Equal(t, constants.PluginTypeSourceReader, detail.PluginType)
}
