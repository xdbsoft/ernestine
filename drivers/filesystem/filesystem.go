package filesystem

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/xdbsoft/ernestine"
)

type store struct {
	basePath string
}

func New(basePath string) (ernestine.Client, error) {

	if err := os.MkdirAll(basePath, 0750); err != nil {
		return nil, err
	}

	return &store{
		basePath: basePath,
	}, nil
}

func (s *store) Create(key string, value []byte, opts ...ernestine.CreateOption) error {
	path := filepath.Join(s.basePath, key)

	return os.WriteFile(path, value, 0666)
}

func (s *store) Get(key string, opts ...ernestine.GetOption) (ernestine.GetResult, error) {
	path := filepath.Join(s.basePath, key)

	f, err := os.Open(path)
	if err != nil {
		return ernestine.GetResult{}, err
	}
	defer f.Close()

	b, err := io.ReadAll(f)
	if err != nil {
		return ernestine.GetResult{}, err
	}

	return ernestine.GetResult{
		Value: b,
	}, nil
}

func (s *store) List(prefix string, opts ...ernestine.ListOption) (ernestine.ListResult, error) {
	dir, err := os.Open(s.basePath)
	if err != nil {
		return ernestine.ListResult{}, err
	}

	entries, err := dir.ReadDir(-1)
	if err != nil {
		return ernestine.ListResult{}, err
	}

	res := ernestine.ListResult{}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), prefix) {
			res.Found += 1
			res.Items = append(res.Items, struct{ Key string }{Key: entry.Name()})
		}
	}
	return res, nil
}

func (s *store) Delete(key string, opts ...ernestine.DeleteOption) error {
	path := filepath.Join(s.basePath, key)

	return os.Remove(path)
}

func (s *store) Cleanup() error {
	return os.RemoveAll(s.basePath)
}
