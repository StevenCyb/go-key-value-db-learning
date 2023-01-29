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

// NewDal creates a new DAL for given file path.
func NewDal(path string) (*DAL, error) {
	dal := &DAL{
		meta:     newEmptyMeta(),
		freelist: newFreelist(),
		pageSize: uint(os.Getpagesize()),
	}
	_, err := os.Stat(path)

	switch {
	case err == nil:
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, fileMode)
		if err != nil {
			_ = dal.Close()

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
			_ = dal.Close()

			return nil, fmt.Errorf("failed to open file: %w", err)
		}

		dal.freelistPageNumber = dal.getNextPage()
		if _, err = dal.writeFreelist(); err != nil {
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

// DAL is the Data Access Layer.
type DAL struct {
	*meta
	*freelist
	file     *os.File
	pageSize uint
}

// Close closes the file.
func (d *DAL) Close() error {
	if d.file == nil {
		return nil
	}

	if err := d.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	return nil
}

// allocateEmptyPage creates a new page object with specified page size.
func (d *DAL) allocateEmptyPage() *page {
	return newPage(d.pageSize)
}

// readPage reads a page with given number from file.
func (d *DAL) readPage(number uint64) (*page, error) {
	allocatedPage := d.allocateEmptyPage()
	offset := uint64(d.pageSize) * number

	if _, err := d.file.ReadAt(allocatedPage.data, int64(offset)); err != nil {
		return nil, fmt.Errorf("failed to read file [%d:%d]: %w", offset, d.pageSize, err)
	}

	return allocatedPage, nil
}

// writePage writes a page to file.
func (d *DAL) writePage(pageToWrite page) error {
	offset := uint64(d.pageSize) * pageToWrite.number

	if _, err := d.file.WriteAt(pageToWrite.data, int64(offset)); err != nil {
		return fmt.Errorf("failed to write file [%d:%d]: %w", offset, d.pageSize, err)
	}

	return nil
}

// writeMeta writes given metadata to first page.
func (d *DAL) writeMeta(metadata meta) (*page, error) {
	metaPage := d.allocateEmptyPage()
	metaPage.number = metaPageNumber

	metadata.serialize(metaPage.data)

	if err := d.writePage(*metaPage); err != nil {
		return nil, fmt.Errorf("failed to write metadata page to file: %w", err)
	}

	return metaPage, nil
}

// readMeta reads metadata from first page.
func (d *DAL) readMeta() (*meta, error) {
	metaPage, err := d.readPage(metaPageNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata page from file: %w", err)
	}

	metadata := newEmptyMeta()
	metadata.deserialize(metaPage.data)

	return metadata, nil
}

// readFreelist reads and deserializes the freelist page.
func (d *DAL) readFreelist() (*freelist, error) {
	freelistPage, err := d.readPage(d.freelistPageNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to read freelist page from file: %w", err)
	}

	freelist := newFreelist()
	freelist.deserialize(freelistPage.data)

	return freelist, nil
}

// writeFreelist serialized freelist and write to page.
func (d *DAL) writeFreelist() (*page, error) {
	freelistPage := d.allocateEmptyPage()
	freelistPage.number = d.freelistPageNumber

	d.freelist.serialize(freelistPage.data)

	if err := d.writePage(*freelistPage); err != nil {
		return nil, fmt.Errorf("failed to write freelist page to file: %w", err)
	}

	return freelistPage, nil
}

// getNode returns a node with given page number.
func (d *DAL) getNode(pageNumber uint64) (*node, error) {
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
func (d *DAL) newNode(items []*item, childNodes []uint64) *node {
	newNode := newEmptyNode()

	newNode.items = items
	newNode.childNodes = childNodes
	newNode.pageNumber = d.getNextPage()
	newNode.dal = d

	return newNode
}

// writeNode writes a node to file.
func (d *DAL) writeNode(nodeToWrite *node) (*node, error) {
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
		return nil, fmt.Errorf("failed to write node page to file: %w", err)
	}

	return nodeToWrite, nil
}

// writeNodes writes all given nodes to file.
func (d *DAL) writeNodes(nodesToWrite ...*node) error {
	for i, nodeToWrite := range nodesToWrite {
		if _, err := d.writeNode(nodeToWrite); err != nil {
			return fmt.Errorf("failed to write nodes (on index %d): %w", i, err)
		}
	}

	return nil
}

// deleteNode delete a node on page with given number.
func (d *DAL) deleteNode(pageNumber uint64) {
	d.releasePage(pageNumber)
}

// isOverPopulated returns if given node is over populated.
func (d *DAL) isOverPopulated(givenNode *node) bool {
	return float32(givenNode.size()) > maxNodeFillPercent*float32(d.pageSize)
}

// isUnderPopulated returns if given node is over under populated.
func (d *DAL) isUnderPopulated(givenNode *node) bool {
	return float32(givenNode.size()) < minNodeFillPercent*float32(d.pageSize)
}

// getSplitIndex should be called when performing rebalance after an item is removed. It checks if a node can spare an
// element, and if it does then it returns the index when there the split should happen. Otherwise -1 is returned.
func (d *DAL) getSplitIndex(givenNode *node) int {
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
