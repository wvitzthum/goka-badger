package badger

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/dgraph-io/badger"
	"github.com/lovoo/goka/storage"
)

const offsetKey = "__offset"

type badgerStorage struct {
	client *badger.DB
}

func New(db *badger.DB) (storage.Storage, error) {
	return &badgerStorage{
		client: db}, nil
}

func (s *badgerStorage) Iterator() (storage.Iterator, error) {
	iter := s.client.NewTransaction(true).
		NewIterator(badger.DefaultIteratorOptions)
	return &badgerIterator{iter: iter}, nil
}

// Iterator returns an iterator that traverses over a snapshot of the storage.
func (s *badgerStorage) IteratorWithRange(start, limit []byte) (storage.Iterator, error) {
	itr := s.client.NewTransaction(true).
		NewIterator(badger.DefaultIteratorOptions)

	for itr.Seek(start); itr.Valid(); itr.Next() {
		item := itr.Item()
		key := item.Key()
		if bytes.Compare(key, limit) > 0 { break }

		}
	return &badgerIterator{iter: itr}, nil
}

func (s *badgerStorage) Has(key string) (bool, error) {
	txn := s.client.NewTransaction(false)
	txn.Commit()
	v, err := txn.Get([]byte(key))
	return v != nil, err
}

func (s *badgerStorage) Get(key string) ([]byte, error) {
	var value []byte
	err := s.client.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			value = append([]byte{}, val...)
			return nil
		})
		return err
	  })
	return value, err
}

func (s *badgerStorage) Set(key string, value []byte) error {
	return s.client.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), []byte(value))
		return err
	  })
}

func (s *badgerStorage) GetOffset(defValue int64) (int64, error) {
	data, err := s.Get(offsetKey)
	if err != nil {
		return 0, err
	}

	if data == nil {
		return defValue, nil
	}

	value, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error decoding offset: %v", err)
	}

	return value, nil
}

func (s *badgerStorage) SetOffset(offset int64) error {
	return s.Set(offsetKey, []byte(strconv.FormatInt(offset, 10)))
}

func (s *badgerStorage) Delete(key string) error {
	return s.client.View(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

func (s *badgerStorage) MarkRecovered() error {
	return nil
}

func (s *badgerStorage) Recovered() bool {
	return false
}

func (s *badgerStorage) Open() error {
	return nil
}

func (s *badgerStorage) Close() error {
	return nil
}
