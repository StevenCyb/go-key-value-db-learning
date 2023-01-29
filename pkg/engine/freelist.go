package engine

// newFreelist creates a new freelist object.
func newFreelist(initialPage uint64) *freelist {
	return &freelist{
		maxPage:       initialPage,
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
