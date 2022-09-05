package badger

import "github.com/dgraph-io/badger"

type badgerIterator struct {
	iter 	*badger.Iterator
}

// Jumps Iterator to next item. In case
// the iterator is done it returns false.
func (i *badgerIterator) Next() bool {
	i.iter.Next()
	return i.iter.Valid()
}

// Returns key of current iterator item.
func (i *badgerIterator) Key() []byte {
	return i.Key()
}

// Not implemented.
func (i *badgerIterator) Err() error {
	return nil
}

// Returns current value of the iterator.
func (i *badgerIterator) Value() ([]byte, error) {
	var valueCopy []byte

	err := 	i.iter.Item().Value(func(v []byte) error {
		valueCopy = append([]byte{}, v...)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return valueCopy, nil
}

// Seek closes the iterator.
func (i *badgerIterator) Release() {
	i.iter.Close()
}

// Seek would seek to the provided key if present.
// If absent, it would seek to the next smallest key
// greater than the provided key if iterating in the
// forward direction. Behavior would be reversed if
// iterating backwards.
func (i *badgerIterator) Seek(key []byte) bool {
	i.iter.Seek(key)
	return true
}
