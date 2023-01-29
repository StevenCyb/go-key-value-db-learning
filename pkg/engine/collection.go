package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	collectionSize = 16
)

var ErrWriteInsideReadTx = errors.New("can't perform a write operation inside a read transaction")

// newCollection creates a new collection with given parameters.
func newCollection(name []byte, root uint64) *Collection {
	return &Collection{
		name: name,
		root: root,
	}
}

// Collection represents a named Collection of key-value pairs.
type Collection struct {
	dal     *dal
	tx      *Transaction
	name    []byte
	root    uint64
	counter uint64
}

func (c *Collection) serialize() *Item {
	bytes := make([]byte, collectionSize)
	leftPos := 0

	binary.LittleEndian.PutUint64(bytes[leftPos:], c.root)

	leftPos += pageNumberSize
	binary.LittleEndian.PutUint64(bytes[leftPos:], c.counter)

	return NewItem(c.name, bytes)
}

func (c *Collection) deserialize(item *Item) {
	c.name = item.key

	if len(item.value) != 0 {
		leftPos := 0

		c.root = binary.LittleEndian.Uint64(item.value[leftPos:])

		leftPos += pageNumberSize
		c.counter = binary.LittleEndian.Uint64(item.value[leftPos:])
	}
}

// getNodes returns a list of nodes based on their indexes (the breadcrumbs) from the root.
//
//	         p
//	     /       \
//	   a          b
//	/     \     /   \
//
// c       d   e     f
// For [0,1,0] -> p,b,e.
func (c *Collection) getNodes(indexes []int) ([]*node, error) {
	root, err := c.dal.getNode(c.root)
	if err != nil {
		return nil, fmt.Errorf("failed to get node: %w", err)
	}

	nodes := []*node{root}
	child := root

	for i := 1; i < len(indexes); i++ {
		child, err = c.dal.getNode(child.childNodes[indexes[i]])
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, child)
	}

	return nodes, nil
}

// Find Returns an item according based on the given key by performing a binary search.
func (c *Collection) Find(key []byte) (*Item, error) {
	n, err := c.dal.getNode(c.root)
	if err != nil {
		return nil, fmt.Errorf("failed to get node: %w", err)
	}

	index, containingNode, _, err := n.findKey(key, true)
	if err != nil {
		return nil, fmt.Errorf("failed to find key: %w", err)
	}

	if index == -1 {
		return nil, nil //nolint:nilnil
	}

	return containingNode.items[index], nil
}

// Put adds a key to the tree. It finds the correct node and the insertion index and adds the item. When performing the
// search, the ancestors are returned as well. This way we can iterate over them to check which nodes were modified and
// rebalance by splitting them accordingly. If the root has too many items, then a new root of a new layer is
// created and the created nodes from the split are added as children.
func (c *Collection) Put(key []byte, value []byte) error { //nolint:funlen,cyclop
	if !c.tx.write {
		return ErrWriteInsideReadTx
	}

	var (
		newItem = NewItem(key, value)
		root    *node
		err     error
	)

	if c.root == 0 {
		root = c.tx.writeNode(c.dal.newNode([]*Item{newItem}, []uint64{}))
		c.root = root.pageNumber

		return nil
	}

	root, err = c.dal.getNode(c.root)
	if err != nil {
		return err
	}

	insertionIndex, nodeToInsertIn, ancestorsIndexes, err := root.findKey(newItem.key, false)
	if err != nil {
		return err
	}

	if nodeToInsertIn.items != nil && bytes.Equal(nodeToInsertIn.items[insertionIndex].key, key) {
		nodeToInsertIn.items[insertionIndex] = newItem
	} else {
		nodeToInsertIn.addItem(newItem, insertionIndex)
	}

	c.tx.writeNode(nodeToInsertIn)

	ancestors, err := c.getNodes(ancestorsIndexes)
	if err != nil {
		return err
	}

	for i := len(ancestors) - 2; i >= 0; i-- { //nolint:gomnd
		pnode := ancestors[i]
		node := ancestors[i+1]
		nodeIndex := ancestorsIndexes[i+1]

		if node.isOverPopulated() {
			pnode.split(node, nodeIndex)
		}
	}

	rootNode := ancestors[0]
	if rootNode.isOverPopulated() {
		newRoot := c.dal.newNode([]*Item{}, []uint64{rootNode.pageNumber})

		newRoot.split(rootNode, 0)

		newRoot = c.tx.writeNode(newRoot)

		c.root = newRoot.pageNumber
	}

	return nil
}

// Remove removes a key from the tree. It finds the correct node and the index to Remove the item from and removes it.
// When performing the search, the ancestors are returned as well. This way we can iterate over them to check which
// nodes were modified and rebalance by rotating or merging the unbalanced nodes. Rotation is done first. If the
// siblings don't have enough items, then merging occurs. If the root is without items after a split, then the root is
// removed and the tree is one level shorter.
func (c *Collection) Remove(key []byte) error { //nolint:cyclop
	if !c.tx.write {
		return ErrWriteInsideReadTx
	}

	rootNode, err := c.dal.getNode(c.root)
	if err != nil {
		return fmt.Errorf("failed to get node: %w", err)
	}

	removeItemIndex, nodeToRemoveFrom, ancestorsIndexes, err := rootNode.findKey(key, true)
	if err != nil {
		return fmt.Errorf("failed to find key in node: %w", err)
	}

	if removeItemIndex == -1 {
		return nil
	}

	if nodeToRemoveFrom.isLeaf() {
		nodeToRemoveFrom.removeItemFromLeaf(removeItemIndex)
	} else {
		var affectedNodes []int

		affectedNodes, err = nodeToRemoveFrom.removeItemFromInternal(removeItemIndex)
		if err != nil {
			return fmt.Errorf("failed to remove item from node: %w", err)
		}

		ancestorsIndexes = append(ancestorsIndexes, affectedNodes...)
	}

	ancestors, err := c.getNodes(ancestorsIndexes)
	if err != nil {
		return fmt.Errorf("failed to get node: %w", err)
	}

	for i := len(ancestors) - 2; i >= 0; i-- { //nolint:gomnd
		pnode := ancestors[i]
		node := ancestors[i+1]

		if node.isUnderPopulated() {
			err = pnode.rebalanceRemove(node, ancestorsIndexes[i+1])
			if err != nil {
				return fmt.Errorf("failed to rebalance node: %w", err)
			}
		}
	}

	rootNode = ancestors[0]
	if len(rootNode.items) == 0 && len(rootNode.childNodes) > 0 {
		c.root = ancestors[1].pageNumber
	}

	return nil
}
