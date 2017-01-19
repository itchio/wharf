package bsdiff

// lexicographic order for pairs
func leq2(a1 int, a2 int, b1 int, b2 int) bool {
	return (a1 < b1 || (a1 == b2 && a2 <= b2))
}

// lexicographic order for triples
func leq3(a1 int, a2 int, a3 int, b1 int, b2 int, b3 int) bool {
	return (a1 < b1 || (a1 == b1 && leq2(a2, a3, b2, b3)))
}

// stably sort a[0..n-1] to b[0..n-1] with keys in 0..K from r
func radixPass(a []int, b []int, r []int, n int, K int) {
	// reset counters
	c := make([]int, K+1)
	for i := 0; i <= K; i++ {
		c[i] = 0
	}

	// count occurences
	for i := 0; i < n; i++ {
		c[r[a[i]]]++
	}

	// exclusive prefix sums
	sum := 0
	for i := 0; i <= K; i++ {
		t := c[i]
		c[i] = sum
		sum += t
	}

	for i := 0; i < n; i++ {
		b[c[r[a[i]]]] = a[i] // sort
		c[r[a[i]]]++
	}
}

// find the suffix array SA of T[0..n-1] in {1..K}^n
// require T[n]=T[n+1]=T[n+2]=0, n>=2
func suffixArray(T []int, SA []int, n int, K int) {
	n0 := (n + 2) / 3
	n1 := (n + 1) / 3
	n2 := n / 3
	n02 := n0 + n2
	R := make([]int, n02+3)
	R[n02] = 0
	R[n02+1] = 0
	R[n02+2] = 0

	SA12 := make([]int, n02+3)
	SA12[n02] = 0
	SA12[n02+1] = 0
	SA12[n02+2] = 0

	R0 := make([]int, n0)
	SA0 := make([]int, n0)

	//******* Step 0: Construct sample ********
	// generate positions of mod 1 and mod 2 suffixes
	// the "+(n0-n1)" adds a dummy mod 1 suffix if n%3 == 1
	{
		i := 0
		j := 0
		for ; i < n+(n0-n1); i++ {
			if i%3 != 0 {
				R[j] = i
				j++
			}
		}
	}

	//******* Step 1: Sort sample suffixes ********
	// lsb radix sort the mod 1 and mod 2 triples
	radixPass(R, SA12, T[2:], n02, K)
	radixPass(SA12, R, T[1:], n02, K)
	radixPass(R, SA12, T, n02, K)

	// find lexicographic names of triples and
	// write them to correct places in R
	name := 0
	c0 := -1
	c1 := -1
	c2 := -1

	for i := 0; i < n02; i++ {
		if T[SA12[i]] != c0 || T[SA12[i]+1] != c1 || T[SA12[i]+2] != c2 {
			name++
			c0 = T[SA12[i]]
			c1 = T[SA12[i]+1]
			c2 = T[SA12[i]+2]
		}

		if SA12[i]%3 == 1 {
			R[SA12[i]/3] = name // write to R1
		} else {
			R[SA12[i]/3+n0] = name // write to R2
		}
	}

	if name < n02 {
		// recurse if names are not yet unique
		suffixArray(R, SA12, n02, name)

		// store unique names in R using the suffix array
		for i := 0; i < n02; i++ {
			R[SA12[i]] = i + 1
		}
	} else {
		// generate the suffix array of R directly
		for i := 0; i < n02; i++ {
			SA12[R[i]-1] = i
		}
	}

	//******* Step 2: Sort nonsample suffixes ********
	// stably sort the mod 0 suffixes from SA12 by their first character
	{
		i := 0
		j := 0
		for ; i < n02; i++ {
			if SA12[i] < n0 {
				R0[j] = 3 * SA12[i]
				j++
			}
		}
	}
	radixPass(R0, SA0, T, n0, K)

	//******* Step 3: Merge ********
	// merge sorted SA0 suffixes and sorted SA12 suffixes
	{
		p := 0
		t := n0 - n1
		k := 0
		GetI := func() int {
			if SA12[t] < n0 {
				return SA12[t]*3 + 1
			}
			return (SA12[t]-n0)*3 + 2
		}

		for ; k < n; k++ {
			i := GetI() // pos of current offset 12 suffix
			j := SA0[p] // pos of current offset 0 suffix

			// different compares for mod 1 and mod 2 suffixes
			var cond bool
			if SA12[t] < n0 {
				cond = leq2(T[i], R[SA12[t]+n0], T[j], R[j/3])
			} else {
				cond = leq3(T[i], T[i+1], R[SA12[t]-n0+1], T[j], T[j+1], R[j/3+n0])
			}

			if cond {
				// suffix from SA12 is smaller
				SA[k] = i
				t++
				if t == n02 {
					// done -- only SA0 suffixes left
					SA[k] = i
					k++
					for p < n0 {
						SA[k] = SA0[p]
						p++
						k++
					}
				}
			} else {
				// suffix from SA0 is smaller
				SA[k] = j
				p++

				if p == n0 {
					// done -- only SA12 suffixes left
					k++
					for t < n02 {
						SA[k] = GetI()
						t++
						k++
					}
				}
			}
		}
	}
}
