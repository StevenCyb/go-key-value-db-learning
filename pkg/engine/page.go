package engine

// newPage creates a new page object.
func newPage(pageSize uint) *page {
	return &page{
		data: make([]byte, pageSize),
	}
}

// page represents the smallest unit of data exchanged by the database and the disk.
type page struct {
	number uint64
	data   []byte
}
