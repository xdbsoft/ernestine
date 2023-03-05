package azblobstorage

import (
	"context"
	"errors"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/xdbsoft/ernestine"
)

type store struct {
	client        *container.Client
	containerName string
}

func New(connectionString string, containerName string) (ernestine.Client, error) {

	client, err := container.NewClientFromConnectionString(connectionString, containerName, nil)
	if err != nil {
		return nil, err
	}

	_, err = client.Create(context.TODO(), &container.CreateOptions{})
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) {
			if respErr.ErrorCode != "ContainerAlreadyExists" {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	return &store{
		client:        client,
		containerName: containerName,
	}, nil
}

func (s *store) Cleanup() error {

	_, err := s.client.Delete(context.TODO(), &container.DeleteOptions{})
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) {
			if respErr.ErrorCode == "ContainerNotFound" {
				return nil
			}
		}
		return err
	}

	return nil
}

func (s *store) Create(key string, value []byte, opts ...ernestine.CreateOption) error {

	_, err := s.client.NewBlockBlobClient(key).UploadBuffer(context.TODO(), value, &blockblob.UploadBufferOptions{})
	if err != nil {
		return err
	}
	return nil

}

func (s *store) Get(key string, opts ...ernestine.GetOption) (ernestine.GetResult, error) {

	resp, err := s.client.NewBlockBlobClient(key).DownloadStream(context.TODO(), &azblob.DownloadStreamOptions{})
	if err != nil {
		return ernestine.GetResult{}, err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return ernestine.GetResult{}, err
	}

	return ernestine.GetResult{
		Value: b,
	}, nil
}

func (s *store) List(prefix string, opts ...ernestine.ListOption) (ernestine.ListResult, error) {

	pager := s.client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	res := ernestine.ListResult{}
	for pager.More() {

		resp, err := pager.NextPage(context.TODO())
		if err != nil {
			return ernestine.ListResult{}, err
		}
		for _, item := range resp.Segment.BlobItems {
			res.Found += 1
			res.Items = append(res.Items, struct{ Key string }{Key: *item.Name})
		}
	}
	return res, nil
}

func (s *store) Delete(key string, opts ...ernestine.DeleteOption) error {

	_, err := s.client.NewBlockBlobClient(key).Delete(context.TODO(), &blob.DeleteOptions{})

	return err
}
