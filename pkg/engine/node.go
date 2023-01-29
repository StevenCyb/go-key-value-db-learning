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
