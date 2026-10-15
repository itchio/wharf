package bsdiff

// subtractInto writes dst[i] = a[i] - b[i] (mod 256) for i in [0, len(dst)).
// a and b must each have at least len(dst) bytes. The hot inner loop is
// unrolled 8-wide so the amd64 backend can keep the pipeline full and
// elide the per-iteration bounds checks — replacing the original
// bytes.Buffer.WriteByte loop in writeMessages.
func subtractInto(dst, a, b []byte) {
	n := len(dst)
	if n == 0 {
		return
	}
	// Bounds-check hints: tell the compiler that a[n-1] and b[n-1] are
	// in range, so it can drop the bounds check on each indexed access
	// inside the loop.
	_ = a[n-1]
	_ = b[n-1]
	i := 0
	for ; i+8 <= n; i += 8 {
		dst[i] = a[i] - b[i]
		dst[i+1] = a[i+1] - b[i+1]
		dst[i+2] = a[i+2] - b[i+2]
		dst[i+3] = a[i+3] - b[i+3]
		dst[i+4] = a[i+4] - b[i+4]
		dst[i+5] = a[i+5] - b[i+5]
		dst[i+6] = a[i+6] - b[i+6]
		dst[i+7] = a[i+7] - b[i+7]
	}
	for ; i < n; i++ {
		dst[i] = a[i] - b[i]
	}
}
