package engine

import (
	"errors"
	"fmt"
	"os"
)

const (
	// fileMode define read and write permissions for everyone.
	fileMode           = os.FileMode(0o666)
	minNodeFillPercent = 0.5
	maxNodeFillPercent = 0.95
)

// newDal creates a new DAL for given file path.
func newDal(path string) (*dal, error) {
	dal := &dal{
		meta:     newEmptyMeta(),
		freelist: newFreelist(),
		pageSize: uint(os.Getpagesize()),
	}
	_, err := os.Stat(path)

	switch {
	case err == nil:
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, fileMode)
		if err != nil {
			_ = dal.close()

			return nil, fmt.Errorf("failed to open file: %w", err)
		}

		dal.meta, err = dal.readMeta()
		if err != nil {
			return nil, err
		}

		dal.freelist, err = dal.readFreelist()
		if err != nil {
			return nil, err
		}
	case errors.Is(err, os.ErrNotExist):
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, fileMode)
		if err != nil {
			_ = dal.close()

			return nil, fmt.Errorf("failed to open file: %w", err)
		}

		dal.freelistPageNumber = dal.getNextPage()
		if err = dal.writeFreelist(); err != nil {
			return nil, err
		}

		// write meta page
		if _, err = dal.writeMeta(*dal.meta); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("failed to get file state: %w", err)
	}

	return dal, nil
}

// dal is the Data Access Layer.
type dal struct {
	*meta
	*freelist
	file     *os.File
	pageSize uint
}

// Close closes the file.
func (d *dal) close() error {
	if d.file == nil {
		return nil
	}

	if err := d.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	return nil
}

// allocateEmptyPage creates a new page object with specified page size.
func (d *dal) allocateEmptyPage() *page {
	return newPage(d.pageSize)
}

// readPage reads a page with given number from file.
func (d *dal) readPage(number uint64) (*page, error) {
	allocatedPage := d.allocateEmptyPage()
	offset := uint64(d.pageSize) * number

	if _, err := d.file.ReadAt(allocatedPage.data, int64(offset)); err != nil {
		return nil, fmt.Errorf("failed to read file [%d:%d]: %w", offset, d.pageSize, err)
	}

	return allocatedPage, nil
}

// writePage writes a page to file.
func (d *dal) writePage(pageToWrite page) error {
	offset := uint64(d.pageSize) * pageToWrite.number

	if _, err := d.file.WriteAt(pageToWrite.data, int64(offset)); err != nil {
		return fmt.Errorf("failed to write file [%d:%d]: %w", offset, d.pageSize, err)
	}

	return nil
}

// writeMeta writes given metadata to first page.
func (d *dal) writeMeta(metadata meta) (*page, error) {
	metaPage := d.allocateEmptyPage()
	metaPage.number = metaPageNumber

	metadata.serialize(metaPage.data)

	if err := d.writePage(*metaPage); err != nil {
		return nil, fmt.Errorf("failed to write metadata page to file: %w", err)
	}

	return metaPage, nil
}

// readMeta reads metadata from first page.
func (d *dal) readMeta() (*meta, error) {
	metaPage, err := d.readPage(metaPageNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata page from file: %w", err)
	}

	metadata := newEmptyMeta()
	metadata.deserialize(metaPage.data)

	return metadata, nil
}

// readFreelist reads and deserializes the freelist page.
func (d *dal) readFreelist() (*freelist, error) {
	freelistPage, err := d.readPage(d.freelistPageNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to read freelist page from file: %w", err)
	}

	freelist := newFreelist()
	freelist.deserialize(freelistPage.data)

	return freelist, nil
}

// writeFreelist serialized freelist and write to page.
func (d *dal) writeFreelist() error {
	freelistPage := d.allocateEmptyPage()
	freelistPage.number = d.freelistPageNumber

	d.freelist.serialize(freelistPage.data)

	if err := d.writePage(*freelistPage); err != nil {
		return fmt.Errorf("failed to write freelist page to file: %w", err)
	}

	return nil
}

// getNode returns a node with given page number.
func (d *dal) getNode(pageNumber uint64) (*node, error) {
	nodePage, err := d.readPage(pageNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to read node page from page %d: %w", pageNumber, err)
	}

	node := newEmptyNode()
	node.deserialize(nodePage.data)
	node.pageNumber = pageNumber

	return node, nil
}

// newNode creates a new node with given items and child nodes.
func (d *dal) newNode(items []*item, childNodes []uint64) *node {
	newNode := newEmptyNode()

	newNode.items = items
	newNode.childNodes = childNodes
	newNode.pageNumber = d.getNextPage()
	newNode.dal = d

	return newNode
}

// writeNode writes a node to file.
func (d *dal) writeNode(nodeToWrite *node) error {
	nodePage := d.allocateEmptyPage()

	if nodeToWrite.pageNumber == 0 {
		nodePage.number = d.getNextPage()
		nodeToWrite.pageNumber = nodePage.number
	} else {
		nodePage.number = nodeToWrite.pageNumber
	}

	nodePage.data = nodeToWrite.serialize(nodePage.data)

	err := d.writePage(*nodePage)
	if err != nil {
		return fmt.Errorf("failed to write node page to file: %w", err)
	}

	return nil
}

// writeNodes writes all given nodes to file.
func (d *dal) writeNodes(nodesToWrite ...*node) error {
	for i, nodeToWrite := range nodesToWrite {
		if err := d.writeNode(nodeToWrite); err != nil {
			return fmt.Errorf("failed to write nodes (on index %d): %w", i, err)
		}
	}

	return nil
}

// deleteNode delete a node on page with given number.
func (d *dal) deleteNode(pageNumber uint64) {
	d.releasePage(pageNumber)
}

// isOverPopulated returns if given node is over populated.
func (d *dal) isOverPopulated(givenNode *node) bool {
	return float32(givenNode.size()) > maxNodeFillPercent*float32(d.pageSize)
}

// isUnderPopulated returns if given node is over under populated.
func (d *dal) isUnderPopulated(givenNode *node) bool {
	return float32(givenNode.size()) < minNodeFillPercent*float32(d.pageSize)
}

// getSplitIndex should be called when performing rebalance after an item is removed. It checks if a node can spare an
// element, and if it does then it returns the index when there the split should happen. Otherwise -1 is returned.
func (d *dal) getSplitIndex(givenNode *node) int {
	size := 0
	size += nodeHeaderSize

	for index := range givenNode.items {
		size += givenNode.items[index].size() + pageNumberSize

		if float32(size) > (minNodeFillPercent*float32(d.pageSize)) && index < len(givenNode.items)-1 {
			return index + 1
		}
	}

	return -1
}
