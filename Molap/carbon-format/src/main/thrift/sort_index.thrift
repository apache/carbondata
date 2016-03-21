struct ColumnSortInfo {
	1: list <list<byte>> sort_index // the surrogate values sorted by the original value order
	2: list <list<byte>> sort_index_inverted // The inverted sort index
}
