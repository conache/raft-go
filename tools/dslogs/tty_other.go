//go:build !unix

package main

// ttyColumns is a stub on non-Unix platforms; the caller falls back to the
// COLUMNS env var or the hardcoded default.
func ttyColumns() int {
	return 0
}
