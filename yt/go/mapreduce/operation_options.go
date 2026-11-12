package mapreduce

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type OperationOption interface {
	isOperationOption()
}

type localFilesOption struct {
	paths []string
}

func (l *localFilesOption) isOperationOption() {}

func (l *localFilesOption) uploadLocalFiles(ctx context.Context, p *prepare) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("failed to upload local file: %w", err)
		}
	}()

	for _, filename := range l.paths {
		st, err := os.Stat(filename)
		if err != nil {
			return err
		}

		f, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer f.Close()

		tmpPath := ypath.Path("//tmp").Child(guid.New().String())

		_, err = p.mr.yc.CreateNode(ctx, tmpPath, yt.NodeFile, nil)
		if err != nil {
			return err
		}

		w, err := p.mr.yc.WriteFile(ctx, tmpPath, &yt.WriteFileOptions{})
		if err != nil {
			return err
		}
		defer w.Close()

		if _, err = io.Copy(w, f); err != nil {
			return err
		}

		if err := w.Close(); err != nil {
			return err
		}

		p.spec.VisitUserScripts(func(script *spec.UserScript) {
			script.FilePaths = append(script.FilePaths, spec.File{
				CypressPath: tmpPath,
				Executable:  st.Mode()&0100 != 0,
				FileName:    filepath.Base(filename),
			})
		})
	}

	return nil
}

// WithLocalFile makes local file available inside job sandbox directory.
//
// Filename and file permissions are preserved. All files are uploaded into job sandbox.
func WithLocalFiles(paths []string) OperationOption {
	return &localFilesOption{paths: paths}
}

type skipSelfUploadOption struct {
}

func (l *skipSelfUploadOption) isOperationOption() {}

func SkipSelfUpload() OperationOption {
	return &skipSelfUploadOption{}
}

type startOperationOptsOption struct {
	options *yt.StartOperationOptions
}

func (o *startOperationOptsOption) isOperationOption() {}

func WithStartOperationOptions(options *yt.StartOperationOptions) OperationOption {
	return &startOperationOptsOption{options: options}
}

type disableIndexControlAttributesOption struct{}

func (o *disableIndexControlAttributesOption) isOperationOption() {}

// disableIndexControlAttributes disables index system columns ($row_index, $range_index)
// in Skiff input format.
func disableIndexControlAttributes() OperationOption {
	return &disableIndexControlAttributesOption{}
}

// WithoutIndexControlAttributes turns off row and range indexes for the operation:
// the Skiff input format omits $row_index and $range_index, and the job I/O
// control attributes of the spec disable enable_row_index and enable_range_index.
// Table index and key switch stay as they are.
//
// Readers of sorted dynamic tables do not return row indexes, so a map or reduce
// over such tables fails with "Row index requested but reader did not return it"
// unless the indexes are off.
func WithoutIndexControlAttributes() OperationOption {
	return disableIndexControlAttributes()
}

type disableGetInputTablesSchemaOption struct{}

func (o *disableGetInputTablesSchemaOption) isOperationOption() {}

// disableGetInputTablesSchema disables automatic retrieval of input table schema.
func disableGetInputTablesSchema() OperationOption {
	return &disableGetInputTablesSchemaOption{}
}
