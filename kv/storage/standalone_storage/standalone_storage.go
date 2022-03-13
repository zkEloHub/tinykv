package standalone_storage

import (
	"path/filepath"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

const (
	dataFileName = "standAloneData"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	kv     *badger.DB
	kvPath string

	// cache engine util
	writeBatch *engine_util.WriteBatch
	// iterator reader list
	readerList []*StandAloneStorageReader
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		kvPath: filepath.Join(conf.DBPath, dataFileName),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.kv = engine_util.CreateDB(s.kvPath, false)
	s.writeBatch = new(engine_util.WriteBatch)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	// close all iterators.
	for _, reader := range s.readerList {
		reader.Close()
	}
	return s.kv.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := NewStandAloneStorageReader(s.kv.NewTransaction(false))
	s.readerList = append(s.readerList, reader)
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writeBatch := s.writeBatch
	defer writeBatch.Reset()

	for _, modify := range batch {
		value := modify.Value()
		if value == nil {
			writeBatch.DeleteCF(modify.Cf(), modify.Key())
		} else {
			writeBatch.SetCF(modify.Cf(), modify.Key(), value)
		}
	}
	return writeBatch.WriteToDB(s.kv)
}

func (s *StandAloneStorage) GetWithCF(ctx *kvrpcpb.Context, cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(s.kv, cf, key)
}
