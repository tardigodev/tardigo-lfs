package main

import (
	"crypto/rand"
	"io"
	"os"
	"path/filepath"

	"github.com/tardigodev/tardigo-core/pkg/constants"
	"github.com/tardigodev/tardigo-core/pkg/objects"
)

type targetWriterPlugin struct {
	DirPath    string
	FileSuffix string
}

func getRandomFileName(charLen uint) string {
	bytes := make([]byte, charLen)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	for i, b := range bytes {
		bytes[i] = b%26 + 'a'
	}
	return string(bytes)
}

func (tp targetWriterPlugin) GetWriter(putWriter func(io.Writer, objects.WriterDetail) error) error {
	filePath := filepath.Join(tp.DirPath, getRandomFileName(10)+tp.FileSuffix)

	writerDetail := objects.WriterDetail{
		WriterTarget: filePath,
		WriterType:   constants.StreamTypeOk,
		WriterError:  nil,
	}

	err := os.MkdirAll(tp.DirPath, os.ModePerm)
	if err != nil {
		writerDetail.WriterType = constants.StreamTypeFailed
		writerDetail.WriterError = err
		if err := putWriter(nil, writerDetail); err != nil {
			return err
		}
	} else {
		file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			writerDetail.WriterType = constants.StreamTypeFailed
			writerDetail.WriterError = err
			if err := putWriter(nil, writerDetail); err != nil {
				return err
			}
		} else {
			if err := putWriter(file, writerDetail); err != nil {
				return err
			}
		}

	}

	return nil
}

func (rp targetWriterPlugin) GetPluginDetail() objects.PluginDetail {
	return objects.PluginDetail{
		PluginName: "local_file_storage_writer",
		PluginType: constants.PluginTypeTargetWriter,
	}
}

// exported
var TargetWriterPlugin = targetWriterPlugin{
	DirPath:    "",
	FileSuffix: "",
}
