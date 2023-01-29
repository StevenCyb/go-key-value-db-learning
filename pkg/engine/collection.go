package engine

import (
	"bytes"
	"fmt"
)

// newCollection creates a new collection with given parameters.
func newCollection(name []byte, root uint64) *collection {
	return &collection{
		name: name,
		root: root,
	}
}

// collection represents a named collection of key-value pairs.
type collection struct {
	dal  *DAL
	name []byte
	root uint64
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
func (c *collection) getNodes(indexes []int) ([]*node, error) {
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
func (c *collection) Find(key []byte) (*item, error) {
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
func (c *collection) Put(key []byte, value []byte) error { //nolint:funlen,cyclop
	var (
		newItem = newItem(key, value)
		root    *node
		err     error
	)

	if c.root == 0 {
		root, err = c.dal.writeNode(c.dal.newNode([]*item{newItem}, []uint64{}))
		if err != nil {
			return err
		}

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

	_, err = c.dal.writeNode(nodeToInsertIn)
	if err != nil {
		return err
	}

	ancestors, err := c.getNodes(ancestorsIndexes)
	if err != nil {
		return err
	}

	for i := len(ancestors) - 2; i >= 0; i-- { //nolint:gomnd
		populateNode := ancestors[i]
		node := ancestors[i+1]
		nodeIndex := ancestorsIndexes[i+1]

		if node.isOverPopulated() {
			if err = populateNode.split(node, nodeIndex); err != nil {
				return err
			}
		}
	}

	// Handle root
	rootNode := ancestors[0]
	if rootNode.isOverPopulated() {
		newRoot := c.dal.newNode([]*item{}, []uint64{rootNode.pageNumber})

		if err = newRoot.split(rootNode, 0); err != nil {
			return err
		}

		// commit newly created root
		newRoot, err = c.dal.writeNode(newRoot)
		if err != nil {
			return err
		}

		c.root = newRoot.pageNumber
	}

	return nil
}
