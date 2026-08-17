package adapters

// KeySet contains the active encryption key and every key retained for reads.
type KeySet struct {
	CurrentKey     []byte
	HistoricalKeys [][]byte
}
