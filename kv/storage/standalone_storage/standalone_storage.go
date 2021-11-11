package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	addr   string
	path   string
	log    *log.Logger
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	log_ := log.New()
	log_.SetLevelByString(conf.LogLevel)

	saDBPath := filepath.Join(conf.DBPath, "saDB")
	os.MkdirAll(saDBPath, os.ModePerm)

	saDB := engine_util.CreateDB(saDBPath, false)

	engine := engine_util.NewEngines(saDB, nil, saDBPath, "")

	ret := StandAloneStorage{addr: conf.StoreAddr, path: conf.DBPath, log: log_, engine: engine}
	return &ret
	//return nil
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.log.Infof("standAloneStorage started...")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	//Your Code Here (1).
	s.log.Infof("standAloneStorage stopped...")
	err := s.engine.Kv.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := &StandAloneStorageReader{txn: s.engine.Kv.NewTransaction(false)}
	if reader != nil {
		return reader, nil
	}
	log.Infof("standAloneStorage get Reader err...")
	return nil, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := &engine_util.WriteBatch{}
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			wb.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			wb.DeleteCF(data.Cf, data.Key)
		}
	}
	return s.engine.WriteKV(wb)
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(sr.txn, cf, key)
}
func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}
func (sr *StandAloneStorageReader) Close() {
	sr.txn.Discard()
}
