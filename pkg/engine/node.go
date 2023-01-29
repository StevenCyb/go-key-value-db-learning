package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	byteOffset  = 1
	int16Offset = 2
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

// NewEmptyNode creates a new node object.
func newEmptyNode() *node {
	return &node{}
}

// node represents a node in a B-Tree.
type node struct {
	*DAL
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
func (n *node) findKey(key []byte) (int, *node, error) {
	index, node, err := findKeyRecursively(n, key)
	if err != nil {
		return -1, nil, fmt.Errorf("failed to find key: %w", err)
	}

	return index, node, nil
}

// findKeyRecursively recursively search for key as follows:
// iterates all the items and finds the key. If the key is found, then the item is returned. If the key
// isn't found then return the index where it should have been (the first index that key is greater than it's previous).
func findKeyRecursively(node *node, key []byte) (int, *node, error) {
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
		return -1, nil, nil
	}

	nextChild, err := node.getNode(node.childNodes[index])
	if err != nil {
		return -1, nil, fmt.Errorf("failed to get child node: %w", err)
	}

	return findKeyRecursively(nextChild, key)
}
