package engine

import "fmt"

// newTransaction creates a new transaction.
func newTransaction(db *DB, write bool) *Transaction {
	return &Transaction{
		db,
		map[uint64]*node{},
		make([]uint64, 0),
		make([]uint64, 0),
		write,
	}
}

// Transaction defines a transaction.
type Transaction struct {
	db                   *DB
	dirtyNodes           map[uint64]*node
	pagesToDelete        []uint64
	allocatedPageNumbers []uint64
	write                bool
}

func (t *Transaction) newNode(items []*item, childNodes []uint64) *node {
	newNode := newEmptyNode()
	newNode.items = items
	newNode.childNodes = childNodes
	newNode.pageNumber = t.db.getNextPage()
	newNode.tx = t

	newNode.tx.allocatedPageNumbers = append(newNode.tx.allocatedPageNumbers, newNode.pageNumber)

	return newNode
}

func (t *Transaction) getNode(pageNum uint64) (*node, error) {
	if node, ok := t.dirtyNodes[pageNum]; ok {
		return node, nil
	}

	node, err := t.db.getNode(pageNum)
	if err != nil {
		return nil, err
	}

	node.tx = t

	return node, nil
}

func (t *Transaction) writeNode(node *node) *node {
	t.dirtyNodes[node.pageNumber] = node
	node.tx = t

	return node
}

// writeNodes writes all given nodes to file.
func (t *Transaction) writeNodes(nodesToWrite ...*node) {
	for _, nodeToWrite := range nodesToWrite {
		t.writeNode(nodeToWrite)
	}
}

func (t *Transaction) deleteNode(node *node) {
	t.pagesToDelete = append(t.pagesToDelete, node.pageNumber)
}

// Rollback undo transaction changes by deleting newly allocated pages and dropping dirty nodes.
func (t *Transaction) Rollback() {
	if !t.write {
		t.db.rwlock.RUnlock()

		return
	}

	t.dirtyNodes = nil
	t.pagesToDelete = nil

	for _, pageNumber := range t.allocatedPageNumbers {
		t.db.freelist.releasePage(pageNumber)
	}

	t.allocatedPageNumbers = nil

	t.db.rwlock.Unlock()
}

// Commit commits changes from dirty node and removing lock.
func (t *Transaction) Commit() error {
	if !t.write {
		t.db.rwlock.RUnlock()

		return nil
	}

	for _, node := range t.dirtyNodes {
		if err := t.db.writeNode(node); err != nil {
			return fmt.Errorf("failed to write dirty node to file: %w", err)
		}
	}

	for _, pageNum := range t.pagesToDelete {
		t.db.deleteNode(pageNum)
	}

	if err := t.db.writeFreelist(); err != nil {
		return fmt.Errorf("failed to write freelist to file: %w", err)
	}

	t.dirtyNodes = nil
	t.pagesToDelete = nil
	t.allocatedPageNumbers = nil

	t.db.rwlock.Unlock()

	return nil
}
