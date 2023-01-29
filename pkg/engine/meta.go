package engine

import "encoding/binary"

const (
	// magicNumber define the file type for this database.
	magicNumber uint32 = 0xD00DB00D
	// metaPageNumber defines the page number for the meta page.
	metaPageNumber = uint64(0)
	// metaPageNumber defines the size of a page number in bytes.
	pageNumberSize = 8
	// magicNumber defines the size of the magic number.
	magicNumberSize = 4
)

// newEmptyMeta creates a new meta object.
func newEmptyMeta() *meta {
	return &meta{}
}

//nolint:godot
// meta is the first page of a database file and holds meta for the database as:
/*
 * freelist meta
 */
type meta struct {
	freelistPageNumber uint64
	rootPageNumber     uint64
}

// serialize given byte array.
func (m *meta) serialize(buffer []byte) {
	pos := 0

	binary.LittleEndian.PutUint32(buffer[pos:], magicNumber)
	pos += magicNumberSize

	binary.LittleEndian.PutUint64(buffer[pos:], m.rootPageNumber)

	pos += pageNumberSize
	binary.LittleEndian.PutUint64(buffer[pos:], m.freelistPageNumber)
}

// deserialize to given byte array.
func (m *meta) deserialize(buffer []byte) {
	pos := 0

	magicNumberRes := binary.LittleEndian.Uint32(buffer[pos:])
	if magicNumberRes != magicNumber {
		panic("The file is not a db file")
	}

	pos += magicNumberSize
	m.rootPageNumber = binary.LittleEndian.Uint64(buffer[pos:])

	pos += pageNumberSize
	m.freelistPageNumber = binary.LittleEndian.Uint64(buffer[pos:])
}
