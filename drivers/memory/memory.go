package memory

import (
	"fmt"
	"strings"

	"github.com/xdbsoft/ernestine"
)

type store struct {
	data map[string][]byte
	keys []string
}

func New() ernestine.Client {
	return &store{
		data: make(map[string][]byte),
	}
}

func (s *store) Cleanup() error {
	s.data = make(map[string][]byte)
	s.keys = nil
	return nil
}

func (s *store) Create(key string, value []byte, opts ...ernestine.CreateOption) error {

	_, found := s.data[key]
	if found {
		return fmt.Errorf("inconsistent call: item with key '%s' already exists", key)
	}

	s.data[key] = value

	s.keys = append(s.keys, key)

	return nil
}

func (s *store) Get(key string, opts ...ernestine.GetOption) (ernestine.GetResult, error) {
	value, found := s.data[key]
	if !found {
		return ernestine.GetResult{}, fmt.Errorf("inconsistent call: item with key '%s' does not exist", key)
	}

	return ernestine.GetResult{
		Value: value,
	}, nil
}

func (s *store) Delete(key string, opts ...ernestine.DeleteOption) error {

	idx := -1
	for i, k := range s.keys {
		if k == key {
			idx = i
		}
	}
	if idx < 0 {
		return fmt.Errorf("inconsistent call: item with key '%s' does not exist", key)
	}

	s.keys = append(s.keys[:idx], s.keys[idx+1:]...)
	delete(s.data, key)
	return nil
}

func (s *store) List(prefix string, opts ...ernestine.ListOption) (ernestine.ListResult, error) {
	res := ernestine.ListResult{
		Found: 0,
	}
	for _, k := range s.keys {
		if strings.HasPrefix(k, prefix) {
			res.Found++
			res.Items = append(res.Items, struct{ Key string }{Key: k})
		}
	}
	return res, nil
}
