//go:build stress

package sipflow

import "testing"

func TestStressBoundedRetention(t *testing.T) {
	stressBoundedRetention(t)
}
