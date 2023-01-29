package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	byteOffset     = 1
	int16Offset    = 2
	nodeHeaderSize = 3
)

// newItem creates a new item object with given key, value pairs.
func newItem(key []byte, value []byte) *item {
	return &item{
		key:   key,
		value: value,
	}
}

// item is a key, value pair in B-Tree node.
type item struct {
	key   []byte
	value []byte
}

// size returns the size of the items in bytes.
func (i item) size() int {
	return len(i.key) + len(i.value)
}

// NewEmptyNode creates a new node object.
func newEmptyNode() *node {
	return &node{}
}

// node represents a node in a B-Tree.
type node struct {
	dal        *DAL
	childNodes []uint64
	items      []*item
	pageNumber uint64
}

// isLeaf returns if node is a leaf.
func (n *node) isLeaf() bool {
	return len(n.childNodes) == 0
}

// serialize serializes the node by converting the data to a slotted page format.
func (n *node) serialize(buffer []byte) []byte {
	leftPos := 0
	rightPos := len(buffer) - 1
	isLeaf := n.isLeaf()

	buffer[leftPos] = byte(0)
	if isLeaf {
		buffer[leftPos] = byte(1)
	}
	leftPos++

	binary.LittleEndian.PutUint16(buffer[leftPos:], uint16(len(n.items)))
	leftPos += int16Offset

	for i := 0; i < len(n.items); i++ {
		item := n.items[i]

		if !isLeaf {
			childNode := n.childNodes[i]

			binary.LittleEndian.PutUint64(buffer[leftPos:], childNode)
			leftPos += pageNumberSize
		}

		keyCount := len(item.key)
		valueCount := len(item.value)

		offset := rightPos - keyCount - valueCount - int16Offset
		binary.LittleEndian.PutUint16(buffer[leftPos:], uint16(offset))
		leftPos += int16Offset

		rightPos -= valueCount
		copy(buffer[rightPos:], item.value)

		rightPos -= byteOffset
		buffer[rightPos] = byte(valueCount)

		rightPos -= byteOffset
		copy(buffer[rightPos:], item.key)

		rightPos -= byteOffset
		buffer[rightPos] = byte(keyCount)
	}

	if !isLeaf {
		lastChildNode := n.childNodes[len(n.childNodes)-1]
		binary.LittleEndian.PutUint64(buffer[leftPos:], lastChildNode)
	}

	return buffer
}

// deserialize deserializes a byte array to node by converting the data from a slotted page format.
func (n *node) deserialize(buffer []byte) {
	leftPos := 1
	isLeaf := buffer[0]

	itemsCount := int(binary.LittleEndian.Uint16(buffer[leftPos : leftPos+int16Offset]))
	leftPos += int16Offset

	for i := 0; i < itemsCount; i++ {
		if isLeaf == 0 { // False
			pageNum := binary.LittleEndian.Uint64(buffer[leftPos:])
			leftPos += pageNumberSize

			n.childNodes = append(n.childNodes, pageNum)
		}

		offset := binary.LittleEndian.Uint16(buffer[leftPos:])
		leftPos += int16Offset

		keyCount := uint16(buffer[int(offset)])
		offset += byteOffset

		key := buffer[offset : offset+keyCount]
		offset += keyCount

		valueCount := uint16(buffer[int(offset)])
		offset += byteOffset

		value := buffer[offset : offset+valueCount]
		n.items = append(n.items, newItem(key, value))
	}

	if isLeaf == 0 {
		pageNum := binary.LittleEndian.Uint64(buffer[leftPos:])
		n.childNodes = append(n.childNodes, pageNum)
	}
}

// findKey searches for a key inside the tree. Once the key is found, the parent node and the correct index are returned
// so the key itself can be accessed in the following way parent[index].
// If the key isn't found, a falsely answer is returned.
func (n *node) findKey(key []byte, exact bool) (int, *node, []int, error) {
	ancestorsIndexes := []int{0}

	index, node, err := findKeyRecursively(n, key, exact, &ancestorsIndexes)
	if err != nil {
		return -1, nil, nil, fmt.Errorf("failed to find key: %w", err)
	}

	return index, node, ancestorsIndexes, nil
}

// findKeyRecursively recursively search for key as follows:
// iterates all the items and finds the key. If the key is found, then the item is returned. If the key
// isn't found then return the index where it should have been (the first index that key is greater than it's previous).
func findKeyRecursively(
	node *node, key []byte, exact bool, ancestorsIndexes *[]int,
) (int, *node, error) {
	wasFound := false
	index := len(node.items)

	for searchIndex, existingItem := range node.items {
		res := bytes.Compare(existingItem.key, key)
		if res == 0 {
			wasFound = true
			index = searchIndex

			break
		} else if res == 1 {
			index = searchIndex

			break
		}
	}

	if wasFound {
		return index, node, nil
	} else if node.isLeaf() {
		if exact {
			return -1, nil, nil
		}

		return index, node, nil
	}

	*ancestorsIndexes = append(*ancestorsIndexes, index)

	nextChild, err := node.dal.getNode(node.childNodes[index])
	if err != nil {
		return -1, nil, fmt.Errorf("failed to get child node: %w", err)
	}

	return findKeyRecursively(nextChild, key, exact, ancestorsIndexes)
}

// nodeSize returns the node's size in bytes.
func (n *node) size() int {
	size := nodeHeaderSize
	for _, item := range n.items {
		size += item.size() + pageNumberSize
	}

	return size
}

func (n *node) addItem(newItem *item, insertionIndex int) int {
	if len(n.items) == insertionIndex {
		n.items = append(n.items, newItem)
	} else {
		n.items = append(n.items[:insertionIndex+1], n.items[insertionIndex:]...)
		n.items[insertionIndex] = newItem
	}

	return insertionIndex
}

// isOverPopulated checks if the node size is bigger than the size of a page.
func (n *node) isOverPopulated() bool {
	return n.dal.isOverPopulated(n)
}

// isUnderPopulated checks if the node size is smaller than the size of a page.
func (n *node) isUnderPopulated() bool {
	return n.dal.isUnderPopulated(n)
}

func (n *node) split(nodeToSplit *node, nodeToSplitIndex int) error {
	splitIndex := nodeToSplit.dal.getSplitIndex(nodeToSplit)
	if splitIndex == -1 {
		return nil
	}

	middleItem := nodeToSplit.items[splitIndex]
	var newNode *node //nolint:wsl

	if nodeToSplit.isLeaf() {
		newNode, _ = n.dal.writeNode(n.dal.newNode(nodeToSplit.items[splitIndex+1:], []uint64{}))
	} else {
		newNode, _ = n.dal.writeNode(n.dal.newNode(nodeToSplit.items[splitIndex+1:], nodeToSplit.childNodes[splitIndex+1:]))
		nodeToSplit.childNodes = nodeToSplit.childNodes[:splitIndex+1]
	}

	nodeToSplit.items = nodeToSplit.items[:splitIndex]

	n.addItem(middleItem, nodeToSplitIndex)

	if len(n.childNodes) == nodeToSplitIndex+1 {
		n.childNodes = append(n.childNodes, newNode.pageNumber)
	} else {
		n.childNodes = append(n.childNodes[:nodeToSplitIndex+1], n.childNodes[nodeToSplitIndex:]...)
		n.childNodes[nodeToSplitIndex+1] = newNode.pageNumber
	}

	if err := n.dal.writeNodes(n, nodeToSplit); err != nil {
		return fmt.Errorf("failed to write nodes: %w", err)
	}

	return nil
}

// removeItemFromLeaf removes an item from a leaf node. It means there is no handling of child nodes.
func (n *node) removeItemFromLeaf(index int) error {
	n.items = append(n.items[:index], n.items[index+1:]...)

	if _, err := n.dal.writeNode(n); err != nil {
		return fmt.Errorf("failed to write node: %w", err)
	}

	return nil
}

// removeItemFromInternal take element before in order (The biggest element from the left branch), put it in the removed
// index and remove it from the original node. Track in affectedNodes any nodes in the path leading to that node.
// It will be used in case the tree needs to be rebalanced.
func (n *node) removeItemFromInternal(index int) ([]int, error) {
	affectedNodes := make([]int, 0)
	affectedNodes = append(affectedNodes, index)

	aNode, err := n.dal.getNode(n.childNodes[index])
	if err != nil {
		return nil, fmt.Errorf("failed to get node: %w", err)
	}

	for !aNode.isLeaf() {
		traversingIndex := len(n.childNodes) - 1

		aNode, err = aNode.dal.getNode(aNode.childNodes[traversingIndex])
		if err != nil {
			return nil, fmt.Errorf("failed to get node: %w", err)
		}

		affectedNodes = append(affectedNodes, traversingIndex)
	}

	n.items[index] = aNode.items[len(aNode.items)-1]
	aNode.items = aNode.items[:len(aNode.items)-1]

	if err := n.dal.writeNodes(n, aNode); err != nil {
		return nil, fmt.Errorf("failed to write nodes: %w", err)
	}

	return affectedNodes, nil
}

// rotateRight rotates the nodes to right to balance the B-Tree.
/*	         p                              p
 *         /   4                          /   3
 *	      /     \           ------>      /     \
 *	   a      b (unbalanced)            a     b (unbalanced)
 *   1,2,3         5                   1,2       4,5.
 */
func rotateRight(aNode, pNode, bNode *node, bNodeIndex int) {
	aNodeItem := aNode.items[len(aNode.items)-1]
	aNode.items = aNode.items[:len(aNode.items)-1]

	pNodeItemIndex := bNodeIndex - 1
	if bNodeIndex == 0 {
		pNodeItemIndex = 0
	}

	pNodeItem := pNode.items[pNodeItemIndex]
	pNode.items[pNodeItemIndex] = aNodeItem

	bNode.items = append([]*item{pNodeItem}, bNode.items...)

	if !aNode.isLeaf() {
		childNodeToShift := aNode.childNodes[len(aNode.childNodes)-1]
		aNode.childNodes = aNode.childNodes[:len(aNode.childNodes)-1]
		bNode.childNodes = append([]uint64{childNodeToShift}, bNode.childNodes...)
	}
}

// rotateLeft rotates the nodes to left to balance the B-Tree.
/* 	         p                                 p
 *         /   2                             /   3
 *	      /      \           ------>        /      \
 *  a(unbalanced)  b                 a(unbalanced)   b
 *   1           3,4,5                   1,2        4,5.
 */
func rotateLeft(aNode, pNode, bNode *node, bNodeIndex int) {
	bNodeItem := bNode.items[0]
	bNode.items = bNode.items[1:]
	pNodeItemIndex := bNodeIndex

	if bNodeIndex == len(pNode.items) {
		pNodeItemIndex = len(pNode.items) - 1
	}

	pNodeItem := pNode.items[pNodeItemIndex]
	pNode.items[pNodeItemIndex] = bNodeItem

	aNode.items = append(aNode.items, pNodeItem)

	if !bNode.isLeaf() {
		childNodeToShift := bNode.childNodes[0]
		bNode.childNodes = bNode.childNodes[1:]
		aNode.childNodes = append(aNode.childNodes, childNodeToShift)
	}
}

// merge merges node if rotation is not possible.
/* 	          p                              p
 *         / 3,5 \                         /   5
 *	      /   |   \       ------>         /     \
 *       a   	b    c                     a       c
 *     1,2    4   6,7                 1,2,3,4   6,7.
 */
func (n *node) merge(bNode *node, bNodeIndex int) error {
	aNode, err := n.dal.getNode(n.childNodes[bNodeIndex-1])
	if err != nil {
		return fmt.Errorf("failed to get node: %w", err)
	}

	pNodeItem := n.items[bNodeIndex-1]
	n.items = append(n.items[:bNodeIndex-1], n.items[bNodeIndex:]...)
	aNode.items = append(aNode.items, pNodeItem)
	aNode.items = append(aNode.items, bNode.items...)
	n.childNodes = append(n.childNodes[:bNodeIndex], n.childNodes[bNodeIndex+1:]...)

	if !aNode.isLeaf() {
		aNode.childNodes = append(aNode.childNodes, bNode.childNodes...)
	}

	if err = n.dal.writeNodes(aNode, n); err != nil {
		return fmt.Errorf("failed to write node: %w", err)
	}

	n.dal.deleteNode(bNode.pageNumber)

	return nil
}

// rebalanceRemove rebalance the tree after a remove operation. This can be either by rotating to the right, to the
// left or by merging. First, the sibling nodes are checked to see if they have enough items for rebalancing
// (>= minItems+1). If they don't have enough items, then merging with one of the sibling nodes occurs. This may leave
// the parent unbalanced by having too little items so rebalancing has to be checked for all the ancestors.
func (n *node) rebalanceRemove(unbalancedNode *node, unbalancedNodeIndex int) error { //nolint:cyclop
	pNode := n

	if unbalancedNodeIndex != 0 {
		leftNode, err := n.dal.getNode(pNode.childNodes[unbalancedNodeIndex-1])
		if err != nil {
			return fmt.Errorf("failed to get node: %w", err)
		}

		if n.dal.getSplitIndex(leftNode) != -1 {
			rotateRight(leftNode, pNode, unbalancedNode, unbalancedNodeIndex)

			if err = n.dal.writeNodes(leftNode, pNode, unbalancedNode); err != nil {
				return fmt.Errorf("failed to write node: %w", err)
			}

			return nil
		}
	}

	if unbalancedNodeIndex != len(pNode.childNodes)-1 {
		rightNode, err := n.dal.getNode(pNode.childNodes[unbalancedNodeIndex+1])
		if err != nil {
			return fmt.Errorf("failed to get node: %w", err)
		}

		if n.dal.getSplitIndex(rightNode) != -1 {
			rotateLeft(unbalancedNode, pNode, rightNode, unbalancedNodeIndex)

			if err = n.dal.writeNodes(unbalancedNode, pNode, rightNode); err != nil {
				return fmt.Errorf("failed to write node: %w", err)
			}

			return nil
		}
	}

	if unbalancedNodeIndex == 0 {
		rightNode, err := n.dal.getNode(n.childNodes[unbalancedNodeIndex+1])
		if err != nil {
			return fmt.Errorf("failed to get node: %w", err)
		}

		return pNode.merge(rightNode, unbalancedNodeIndex+1)
	}

	return pNode.merge(unbalancedNode, unbalancedNodeIndex)
}
