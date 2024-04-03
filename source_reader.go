package main

import (
	"io"
	"os"

	"github.com/tardigodev/tardigo-core/pkg/constants"
	"github.com/tardigodev/tardigo-core/pkg/objects"
)

type sourceReaderPlugin struct {
	Paths []string
}

func (rp sourceReaderPlugin) GetReader(putReader func(io.Reader, objects.ReaderDetail) error) error {
	for _, path := range rp.Paths {
		readerDetail := objects.ReaderDetail{
			ReaderSource: path,
			ReaderType:   constants.StreamTypeOk,
			ReaderError:  nil,
		}

		file, err := os.Open(path)
		if err != nil {
			readerDetail.ReaderType = constants.StreamTypeFailed
			readerDetail.ReaderError = err
			if err := putReader(nil, readerDetail); err != nil {
				return err
			}
		} else {
			if err := putReader(file, readerDetail); err != nil {
				return err
			}
		}

	}
	putReader(nil, objects.ReaderDetail{
		ReaderSource: "",
		ReaderType:   constants.StreamTypeEnd,
		ReaderError:  nil,
	})
	return nil
}

func (rp sourceReaderPlugin) GetPluginDetail() objects.PluginDetail {
	return objects.PluginDetail{
		PluginName: "local_file_storage_reader",
		PluginType: constants.PluginTypeSourceReader,
	}
}

// exported
var SourceReaderPlugin = sourceReaderPlugin{
	Paths: []string{},
}
