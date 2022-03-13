package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	resp := &kvrpcpb.RawGetResponse{}
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	resp.Value = val
	if val == nil {
		resp.NotFound = true
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	batch := storage.Modify{
		Data: storage.Put{
			Cf:    req.Cf,
			Key:   req.Key,
			Value: req.Value,
		},
	}
	err := server.storage.Write(nil, []storage.Modify{batch})
	resp := &kvrpcpb.RawPutResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	batch := storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	}
	resp := &kvrpcpb.RawDeleteResponse{}
	err := server.storage.Write(nil, []storage.Modify{batch})
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	resp := &kvrpcpb.RawScanResponse{}
	iterator := reader.IterCF(req.Cf)
	defer iterator.Close()

	limit := req.Limit
	for iterator.Seek(req.StartKey); iterator.Valid() && limit != 0; iterator.Next() {
		item := iterator.Item()
		val, vErr := item.Value()
		pair := &kvrpcpb.KvPair{
			Key: item.Key(),
		}
		if vErr != nil {
			resp.Error = vErr.Error()
			continue
		}
		pair.Value = val
		resp.Kvs = append(resp.Kvs, pair)
		limit--
	}
	return resp, nil
}
