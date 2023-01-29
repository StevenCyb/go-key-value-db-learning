package engine

import (
	"fmt"
	"os"
)

// NewDal creates a new DAL for given file path.
func NewDal(path string, pageSize uint) (*DAL, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	return &DAL{file: file, pageSize: pageSize, freelist: newFreelist(0)}, nil
}

// DAL is the Data Access Layer.
type DAL struct {
	file     *os.File
	pageSize uint
	*freelist
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
func (d *DAL) writePage(p page) error {
	offset := uint64(d.pageSize) * p.number

	if _, err := d.file.WriteAt(p.data, int64(offset)); err != nil {
		return fmt.Errorf("failed to write file [%d:%d]: %w", offset, d.pageSize, err)
	}

	return nil
}
