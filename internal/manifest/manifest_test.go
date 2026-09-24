package manifest

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v2"
)

func TestYAMLArtifactsParse(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	paths := []string{filepath.Join(root, "api"), filepath.Join(root, "deploy")}
	files := []string{filepath.Join(root, "docker-compose.yml")}
	for _, directory := range paths {
		err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() && (strings.HasSuffix(path, ".yaml") || strings.HasSuffix(path, ".yml")) {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", directory, err)
		}
	}
	for _, path := range files {
		path := path
		t.Run(strings.TrimPrefix(path, root+string(filepath.Separator)), func(t *testing.T) {
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			decoder := yaml.NewDecoder(file)
			for document := 1; ; document++ {
				var value any
				err := decoder.Decode(&value)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("document %d: %v", document, err)
				}
			}
		})
	}
}
