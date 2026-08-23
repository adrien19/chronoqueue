//go:build !cgo

package sqlite

func isUniqueConstraintError(error) bool {
	return false
}
