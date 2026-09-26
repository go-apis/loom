package sdl

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"

	"github.com/go-apis/loom/schema"
)

// ParseDir parses every *.loom file under root, at any depth, as one
// schema. Files are ordered by their slash-separated path relative to
// root, so the result does not depend on how the filesystem lists them.
// Errors name each file as root joined with its relative path.
func ParseDir(root string) (*schema.Schema, error) {
	return parseFS(os.DirFS(root), root)
}

// ParseFS is ParseDir over any file system (an embed.FS, say). Errors
// name each file by its path within fsys.
func ParseFS(fsys fs.FS) (*schema.Schema, error) {
	return parseFS(fsys, "")
}

func parseFS(fsys fs.FS, root string) (*schema.Schema, error) {
	var rels []string
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && path.Ext(p) == ".loom" {
			rels = append(rels, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(rels) == 0 {
		return nil, fmt.Errorf("no *.loom files under %s", filepath.Join(root, "."))
	}
	sort.Strings(rels)
	files := make([]File, len(rels))
	for i, rel := range rels {
		src, err := fs.ReadFile(fsys, rel)
		if err != nil {
			return nil, err
		}
		name := rel
		if root != "" {
			name = filepath.Join(root, filepath.FromSlash(rel))
		}
		files[i] = File{Path: name, Src: string(src)}
	}
	return ParseFiles(files)
}

// ParsePaths reads the named files and parses them, in the order given,
// as one schema (see ParseFiles).
func ParsePaths(paths []string) (*schema.Schema, error) {
	files := make([]File, len(paths))
	for i, p := range paths {
		src, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		files[i] = File{Path: p, Src: string(src)}
	}
	return ParseFiles(files)
}
