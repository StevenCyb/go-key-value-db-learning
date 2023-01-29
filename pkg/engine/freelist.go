package engine

import "encoding/binary"

// newFreelist creates a new freelist object.
func newFreelist() *freelist {
	return &freelist{
		maxPage:       metaPageNumber,
		releasedPages: []uint64{},
	}
}

// freelist helps to organize pages by tracing the last and freed pages.
// This is important to reuse freed pages and to avoid fragmentation.
type freelist struct {
	releasedPages []uint64
	maxPage       uint64
}

// getNextPage returns a freed page number or a new one if no freed pages exist.
func (f *freelist) getNextPage() uint64 {
	if len(f.releasedPages) > 0 {
		index := len(f.releasedPages) - 1
		pageNumber := f.releasedPages[index]
		f.releasedPages = f.releasedPages[:index]

		return pageNumber
	}

	f.maxPage++

	return f.maxPage
}

// releasePage marks given page number as freed.
func (f *freelist) releasePage(number uint64) {
	f.releasedPages = append(f.releasedPages, number)
}

// serialize serializes the freelist object into byte array.
func (f *freelist) serialize(buffer []byte) []byte {
	pos := 0

	binary.LittleEndian.PutUint64(buffer[pos:], f.maxPage)
	pos += pageNumberSize

	// released pages count
	binary.LittleEndian.PutUint64(buffer[pos:], uint64(len(f.releasedPages)))
	pos += pageNumberSize

	for _, page := range f.releasedPages {
		binary.LittleEndian.PutUint64(buffer[pos:], page)
		pos += pageNumberSize
	}

	return buffer
}

// deserialize deserializes the byte array to freelist object.
func (f *freelist) deserialize(buf []byte) {
	pos := 0
	f.maxPage = binary.LittleEndian.Uint64(buf[pos:])
	pos += pageNumberSize

	// released pages count
	releasedPagesCount := binary.LittleEndian.Uint64(buf[pos:])
	pos += pageNumberSize

	for i := uint64(0); i < releasedPagesCount; i++ {
		f.releasedPages = append(f.releasedPages, binary.LittleEndian.Uint64(buf[pos:]))
		pos += pageNumberSize
	}
}
