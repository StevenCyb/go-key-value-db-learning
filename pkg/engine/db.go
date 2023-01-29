package engine

import "sync"

// DB is the interface of the database.
type DB struct {
	*dal
	rwlock sync.RWMutex
}

// Open the database for given path.
func Open(path string) (*DB, error) {
	var err error

	dal, err := newDal(path)
	if err != nil {
		return nil, err
	}

	db := &DB{
		dal,
		sync.RWMutex{},
	}

	return db, nil
}

// Close closes the database.
func (db *DB) Close() error {
	return db.dal.close()
}

// ReadTransaction create a new read transaction.
func (db *DB) ReadTransaction() *Transaction {
	db.rwlock.RLock()

	return newTransaction(db, false)
}

// WriteTransaction create a new write transaction.
func (db *DB) WriteTransaction() *Transaction {
	db.rwlock.Lock()

	return newTransaction(db, true)
}
