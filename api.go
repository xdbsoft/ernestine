package ernestine

type Client interface {
	Create(key string, value []byte, opts ...CreateOption) error
	Get(key string, opts ...GetOption) (GetResult, error)
	Delete(key string, opts ...DeleteOption) error
	List(prefix string, opts ...ListOption) (ListResult, error)
	Cleanup() error
}

type CreateOption interface{}
type GetOption interface{}
type DeleteOption interface{}
type ListOption interface{}

type GetResult struct {
	Value []byte
}

type ListResult struct {
	Found int
	Items []struct {
		Key string
	}
}
