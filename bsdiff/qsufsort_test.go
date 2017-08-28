package bsdiff

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"runtime"
	"testing"

	"bytes"

	"index/suffixarray"

	"github.com/stretchr/testify/assert"
	"github.com/itchio/wharf/state"
	"github.com/jgallagher/gosaca"
)

func Test_QsufsortSeq(t *testing.T) {
	testQsufsort(t, 0)
}

func Test_QsufsortPar2(t *testing.T) {
	testQsufsort(t, 2)
}

func Test_QsufsortPar4(t *testing.T) {
	testQsufsort(t, 4)
}

func Test_QsufsortPar8(t *testing.T) {
	testQsufsort(t, 8)
}

var dictwords []byte
var dictcalls []byte

var result []int

func testQsufsort(t *testing.T, concurrency int) {
	input := paper

	ctx := &DiffContext{
		SuffixSortConcurrency: concurrency,
	}
	consumer := &state.Consumer{}

	I := qsufsort(input, ctx, consumer)

	for i := range I {
		if i == 0 {
			continue
		}

		prev := input[I[i-1]:]
		next := input[I[i]:]
		assert.EqualValues(t, -1, bytes.Compare(prev, next))
	}
}

func benchQsuf(input []byte, concurrency int, b *testing.B) {
	ctx := &DiffContext{SuffixSortConcurrency: concurrency}
	consumer := &state.Consumer{}

	var r []int
	for n := 0; n < b.N; n++ {
		r = qsufsort(input, ctx, consumer)
	}
	result = r
}

var sa *suffixarray.Index

func benchSuffixarray(input []byte, b *testing.B) {
	for n := 0; n < b.N; n++ {
		sa = suffixarray.New(input)
	}
}

var saz *SuffixArrayZ

func benchSuffixarrayz(input []byte, b *testing.B) {
	for n := 0; n < b.N; n++ {
		NewSuffixArrayZ(input)
	}
}

func benchGosaca(input []byte, b *testing.B) {
	for n := 0; n < b.N; n++ {
		if len(input) == 0 {
			return
		}

		ws := &gosaca.WorkSpace{}
		SA := make([]int, len(input))
		ws.ComputeSuffixArray(input, SA)
	}
}

func benchGosacaPar(input []byte, b *testing.B, numWorkers int) {
	for n := 0; n < b.N; n++ {
		if len(input) == 0 {
			return
		}

		done := make(chan bool)

		boundaries := make([]int, numWorkers+1)
		boundary := 0

		for i := 0; i < numWorkers; i++ {
			boundaries[i] = boundary
			boundary += len(input) / numWorkers
		}
		boundaries[numWorkers] = len(input)

		SA := make([]int, len(input)+1)

		for i := 0; i < numWorkers; i++ {
			st := boundaries[i]
			en := boundaries[i+1]

			go func() {
				ws := &gosaca.WorkSpace{}
				ws.ComputeSuffixArray(input[st:en], SA[st:en])
				done <- true
			}()
		}

		for i := 0; i < numWorkers; i++ {
			<-done
		}
	}
}

func Benchmark_Qsufsort(b *testing.B) {
	// note: 'paper' is not worth benchmarking because it's too short
	var datasets = []struct {
		name string
		data []byte
	}{
		{"dictwords", dictwords},
		{"dictcalls", dictcalls},
	}

	for _, dataset := range datasets {
		for _, concurrency := range []int{1, 2, 4, 8} {
			testName := fmt.Sprintf("gosaca-%d-parts-%s", concurrency, dataset.name)
			b.Run(testName, func(b *testing.B) {
				if concurrency == 1 {
					benchGosaca(dataset.data, b)
				} else {
					benchGosacaPar(dataset.data, b, concurrency)
				}
			})
		}
	}

	for _, dataset := range datasets {
		for _, concurrency := range []int{0, 2, 8} {
			testName := fmt.Sprintf("qsufsort-%s-j%d", dataset.name, concurrency)
			b.Run(testName, func(b *testing.B) {
				benchQsuf(dataset.data, concurrency, b)
			})
		}
	}
}

func init() {
	_, filename, _, _ := runtime.Caller(0)

	var err error
	dictwords, err = ioutil.ReadFile(filepath.Join(filepath.Dir(filename), "dictwords"))
	if err != nil {
		fmt.Printf("Could not load dictwords, benchmarks won't be functional (see README.md)\n")
	}

	dictcalls, err = ioutil.ReadFile(filepath.Join(filepath.Dir(filename), "dictcalls"))
	if err != nil {
		fmt.Printf("Could not load dictcalls, benchmarks won't be functional (see README.md)\n")
	}
}

var paper []byte = []byte(`
    Quicksort is a textbook divide-and-conquer algorithm.
    To sort an array, choose a partitioning element, permute
    the elements such that lesser elements are on one side and
    greater elements are on the other, and then recursively sort
    the two subarrays. But what happens to elements equal to
    the partitioning value? Hoare’s partitioning method is
    binary: it places lesser elements on the left and greater elements
    on the right, but equal elements may appear on
    either side.

    Algorithm designers have long recognized the desirability
    and difficulty of a ternary partitioning method.
    Sedgewick [22] observes on page 244: ‘‘Ideally, we would
    like to get all [equal keys] into position in the file, with all
    the keys with a smaller value to their left, and all the keys
    with a larger value to their right. Unfortunately, no
    efficient method for doing so has yet been devised....’’
    Dijkstra [6] popularized this as ‘‘The Problem of the Dutch
    National Flag’’: we are to order a sequence of red, white
    and blue pebbles to appear in their order on Holland’s
    ensign. This corresponds to Quicksort partitioning when
    lesser elements are colored red, equal elements are white,
    and greater elements are blue. Dijkstra’s ternary algorithm
    requires linear time (it looks at each element exactly once),
    but code to implement it has a significantly larger constant
    factor than Hoare’s binary partitioning code.

    Wegner [27] describes more efficient ternary partitioning
    schemes. Bentley and McIlroy [2] present a ternary
    partition based on this counterintuitive loop invariant:

    The main partitioning loop has two inner loops. The first
    inner loop moves up the index b: it scans over lesser elements,
    swaps equal elements to a, and halts on a greater
    element. The second inner loop moves down the index c
    correspondingly: it scans over greater elements, swaps
    equal elements to d, and halts on a lesser element. The
    main loop then swaps the elements pointed to by b and c,
    increments b and decrements c, and continues until b and
    c cross. (Wegner proposed the same invariant, but maintained
    it with more complex code.) Afterwards, the equal
    elements on the edges are swapped to the middle of the
    array, without any extraneous comparisons. This code partitions
    an n-element array using n − 1 comparisons

    Quicksort has been extensively analyzed by authors
    including Hoare [9], van Emden [26], Knuth [11], and
    Sedgewick [23]. Most detailed analyses involve the harmonic
    numbers Hn = Σ 1≤i≤n 1/ i.

    Theorem 1. [Hoare] A Quicksort that partitions
    around a single randomly selected element sorts n distinct
    items in 2nHn + O(n) ∼∼ 1. 386n lg n expected
    comparisons.

    A common variant of Quicksort partitions around the
    median of a random sample.

    Theorem 2. [van Emden] A Quicksort that partitions
    around the median of 2t + 1 randomly selected elements
    sorts n distinct items in 2nHn / (H2t + 2 − Ht + 1 )
    + O(n) expected comparisons.
    By increasing t, we can push the expected number of comparisons
    close to n lg n + O(n).
    `)
