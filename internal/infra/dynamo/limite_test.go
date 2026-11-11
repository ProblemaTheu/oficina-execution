package dynamo

import (
	"math"
	"testing"
)

func TestLimite32(t *testing.T) {
	for n, esperado := range map[int]int32{0: 1, -5: 1, 50: 50, math.MaxInt32 + 10: math.MaxInt32} {
		if got := limite32(n); got != esperado {
			t.Errorf("limite32(%d) = %d, esperava %d", n, got, esperado)
		}
	}
}
