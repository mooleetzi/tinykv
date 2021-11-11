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
	reader, _ := server.storage.Reader(nil)
	defer reader.Close()
	resp := new(kvrpcpb.RawGetResponse)
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	resp.Value = val
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	resp := new(kvrpcpb.RawPutResponse)
	modify := []storage.Modify{
		{Data: storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}},
	}
	err := server.storage.Write(nil, modify)
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	resp := new(kvrpcpb.RawDeleteResponse)
	modify := []storage.Modify{
		{Data: storage.Delete{Key: req.Key, Cf: req.Cf}},
	}
	err := server.storage.Write(nil, modify)
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := new(kvrpcpb.RawScanResponse)
	reader, _ := server.storage.Reader(nil)
	iter := reader.IterCF(req.GetCf())
	defer reader.Close()
	defer iter.Close()
	for i := uint32(0); iter.Valid() && i < req.Limit; i++ {
		item := iter.Item()
		key := item.Key()
		value, err := item.Value()
		if err != nil {
			//log
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{Key: key, Value: value})
		iter.Next()
	}
	return resp, nil
}
