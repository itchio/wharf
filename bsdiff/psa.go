package bsdiff

import "github.com/jgallagher/gosaca"

// Partitioned suffix array
type PSA struct {
	p          int
	buf        []byte
	boundaries []int

	I    []int
	done chan bool
}

type BucketGroup struct {
	numSuffixes   int
	bucketNumbers []int
}

func NewPSA(p int, buf []byte) *PSA {
	boundaries := make([]int, p+1)
	boundary := 0
	partitionSize := len(buf) / p

	for i := 0; i < p; i++ {
		boundaries[i] = boundary
		boundary += partitionSize
	}
	boundaries[p] = len(buf)

	sortDone := make(chan bool)
	I := make([]int, len(buf))

	// bucketPrefixCount := make([]int, 256)
	// for i := 0; i < len(buf); i++ {
	// 	c := buf[i]
	// 	bucketPrefixCount[c]++
	// }

	// beforeDistrib := time.Now()

	// bucketDistribution := make([]BucketGroup, p)

	// for bucketIndex, bucketSize := range bucketPrefixCount {
	// 	smallestGroupSize := len(buf)
	// 	smallestGroupIndex := -1

	// 	for groupIndex, group := range bucketDistribution {
	// 		if group.numSuffixes < smallestGroupSize {
	// 			smallestGroupSize = group.numSuffixes
	// 			smallestGroupIndex = groupIndex
	// 		}
	// 	}

	// 	group := bucketDistribution[smallestGroupIndex]
	// 	group.numSuffixes += bucketSize
	// 	group.bucketNumbers = append(group.bucketNumbers, bucketIndex)
	// 	bucketDistribution[smallestGroupIndex] = group
	// }

	// fmt.Fprintf(os.Stderr, "computed bucket distribution in %s\n", time.Since(beforeDistrib))

	// fmt.Fprintf(os.Stderr, "bucket prefix counts: %v\n", bucketPrefixCount)

	// fmt.Fprintf(os.Stderr, "bucket distribution: \n")
	// for _, group := range bucketDistribution {
	// 	fmt.Fprintf(os.Stderr, " - %d suffixes (%d buckets)\n", group.numSuffixes, len(group.bucketNumbers))
	// }

	for i := 0; i < p; i++ {
		go func(i int) {
			st := boundaries[i]
			en := boundaries[i+1]
			ws := &gosaca.WorkSpace{}
			ws.ComputeSuffixArray(buf[st:en], I[st:en])
			sortDone <- true
		}(i)
	}

	for i := 0; i < p; i++ {
		<-sortDone
	}

	psa := &PSA{
		p:          p,
		buf:        buf,
		I:          I,
		boundaries: boundaries,
	}

	return psa
}

func (psa *PSA) search(nbuf []byte) (pos, n int) {
	var bpos, bn, i int

	for i = 0; i < psa.p; i++ {
		st := psa.boundaries[i]
		en := psa.boundaries[i+1]

		ppos, pn := search(psa.I[st:en], psa.buf[st:en], nbuf, 0, en-st)
		if pn > bn {
			bn = pn
			bpos = ppos + st
		}
	}

	return bpos, bn
}
