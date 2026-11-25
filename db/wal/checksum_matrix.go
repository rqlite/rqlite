package wal

// The SQLite WAL checksum is updated 8 bytes at a time. For an 8-byte block
// holding the 32-bit words (x, y), the running checksum (s0, s1) advances as
//
//	s0' = s0 + x + s1
//	s1' = s1 + y + s0'
//
// which is the affine map
//
//	[s0']   [1 1] [s0]   [x    ]
//	[s1'] = [1 2] [s1] + [x + y]
//
// The linear part is therefore A^n after n 8-byte blocks, where
//
//	A = [1 1]
//	    [1 2]

// checksumMatrix is a 2x2 matrix in row-major order (a00, a01, a10, a11).
type checksumMatrix [4]uint32

// checksumMatrixPow returns A^n, the linear part after n 8-byte blocks.
// Arithmetic wraps at 2^32, matching the checksum itself.
//
// Each column is the image of a basis vector, so both columns are obtained by
// running the two additions above n times. n is fixed per Writer and at most
// 1+pageSize/8, so this costs nothing measurable.
func checksumMatrixPow(n uint64) checksumMatrix {
	s00, s10 := uint32(1), uint32(0) // image of (1, 0)
	s01, s11 := uint32(0), uint32(1) // image of (0, 1)
	for range n {
		s00, s10 = s00+s10, s00+2*s10
		s01, s11 = s01+s11, s01+2*s11
	}
	return checksumMatrix{s00, s01, s10, s11}
}

// apply returns a*(v0, v1).
func (a checksumMatrix) apply(v0, v1 uint32) [2]uint32 {
	return [2]uint32{a[0]*v0 + a[1]*v1, a[2]*v0 + a[3]*v1}
}
