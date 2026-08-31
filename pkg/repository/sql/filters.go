package sql

type PriorityRange struct {
	Min int64
	Max int64
}

type PeekCursor struct {
	Priority int64
	RowID    int64
}
