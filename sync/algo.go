// Base on code from: https://bitbucket.org/kardianos/rsync/
// Original algorithm: http://www.samba.org/~tridge/phd_thesis.pdf
//
// Definitions
//   Source: The final content.
//   Target: The content to be made into final content.
//   Signature: The sequence of hashes used to identify the content.
package sync

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
)

const MaxDataOp = (4 * 1024 * 1024)
const DEBUG_SYNC = false

func NewContext(BlockSize int) *SyncContext {
	return &SyncContext{
		blockSize:    BlockSize,
		uniqueHasher: md5.New(),
	}
}

// Apply the difference to the target.
func (ctx *SyncContext) ApplyRecipe(output io.Writer, pool FilePool, ops chan Operation) error {
	var err error
	var n int
	var block []byte

	minBufferSize := ctx.blockSize
	if len(ctx.buffer) < minBufferSize {
		ctx.buffer = make([]byte, minBufferSize)
	}
	buffer := ctx.buffer

	writeBlock := func(op Operation) error {
		target, err := pool.GetReader(op.FileIndex)
		if err != nil {
			return err
		}

		target.Seek(int64(ctx.blockSize*int(op.BlockIndex)), os.SEEK_SET)
		n, err = io.ReadAtLeast(target, buffer, ctx.blockSize)
		if err != nil {
			// UnexpectedEOF is actually expected, since we want to copy short
			// blocks at the end of files. Other errors aren't expected.
			if err != io.ErrUnexpectedEOF {
				return err
			}
		}
		block = buffer[:n]
		_, err = output.Write(block)
		if err != nil {
			return err
		}
		return nil
	}

	for op := range ops {
		switch op.Type {
		case OpBlockRange:
			for i := int64(0); i < op.BlockSpan; i++ {
				BlockIndex := op.BlockIndex + i

				err = writeBlock(Operation{
					Type:       OpBlock,
					FileIndex:  op.FileIndex,
					BlockIndex: BlockIndex,
				})
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
			}
		case OpBlock:
			err = writeBlock(op)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
		case OpData:
			_, err = output.Write(op.Data)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Create the operation list to mutate the target signature into the source.
// Any data operation from the OperationWriter must have the data copied out
// within the span of the function; the data buffer underlying the operation
// data is reused.
func (ctx *SyncContext) ComputeDiff(source io.Reader, library *BlockLibrary, ops OperationWriter) (err error) {
	minBufferSize := (ctx.blockSize * 2) + MaxDataOp
	if len(ctx.buffer) < minBufferSize {
		ctx.buffer = make([]byte, minBufferSize)
	}
	buffer := ctx.buffer

	type section struct {
		tail int
		head int
	}

	var data, sum section
	var n, validTo int
	var αPop, αPush, β, β1, β2 uint32
	var rolling, lastRun bool
	var shortSize int32 = 0

	// Store the previous non-data operation for combining.
	var prevOp *Operation

	// Send the last operation if there is one waiting.
	defer func() {
		if prevOp == nil {
			return
		}

		err = ops(*prevOp)
		prevOp = nil
	}()

	// Combine OpBlock into OpBlockRange. To achieve this, we store the previous
	// non-data operation and determine if it can be extended.
	enqueue := func(op Operation) (err error) {
		switch op.Type {
		case OpBlock:
			if prevOp != nil {
				if prevOp.FileIndex == op.FileIndex {
					switch prevOp.Type {
					case OpBlock:
						if prevOp.BlockIndex+1 == op.BlockIndex {
							prevOp = &Operation{
								Type:       OpBlockRange,
								FileIndex:  prevOp.FileIndex,
								BlockIndex: prevOp.BlockIndex,
								BlockSpan:  2,
							}
							return
						}
					case OpBlockRange:
						if prevOp.BlockIndex+prevOp.BlockSpan == op.BlockIndex {
							prevOp.BlockSpan++
							return
						}
					}
				}
				err = ops(*prevOp)
				if err != nil {
					return
				}
				prevOp = nil
			}
			prevOp = &op
		case OpData:
			// Never save a data operation, as it would corrupt the buffer.
			if prevOp != nil {
				err = ops(*prevOp)
				if err != nil {
					return
				}
			}
			err = ops(op)
			if err != nil {
				return
			}
			prevOp = nil
		}
		return
	}

	for !lastRun {
		// Determine if the buffer should be extended.
		if sum.tail+ctx.blockSize > validTo {
			// Determine if the buffer should be wrapped.
			if validTo+ctx.blockSize > len(buffer) {
				// Before wrapping the buffer, send any trailing data off.
				if data.tail < data.head {
					err = enqueue(Operation{Type: OpData, Data: buffer[data.tail:data.head]})
					if err != nil {
						return err
					}
				}
				// Wrap the buffer.
				l := validTo - sum.tail
				copy(buffer[:l], buffer[sum.tail:validTo])

				// Reset indexes.
				validTo = l
				sum.tail = 0
				data.head = 0
				data.tail = 0
			}

			n, err = io.ReadAtLeast(source, buffer[validTo:validTo+ctx.blockSize], ctx.blockSize)
			if DEBUG_SYNC {
				fmt.Printf("read %d\n", n)
			}
			validTo += n
			if err != nil {
				if err != io.EOF && err != io.ErrUnexpectedEOF {
					return err
				}
				lastRun = true

				shortSize = int32(n)
				if DEBUG_SYNC {
					fmt.Printf("last block size = %d\n", shortSize)
				}
			}
			if n == 0 {
				break
			}
		}

		// Set the hash sum window head. Must either be a block size
		// or be at the end of the buffer.
		sum.head = min(sum.tail+ctx.blockSize, validTo)

		// Compute the rolling hash.
		if !rolling {
			β, β1, β2 = βhash(buffer[sum.tail:sum.head])
			rolling = true
		} else {
			αPush = uint32(buffer[sum.head-1])
			β1 = (β1 - αPop + αPush) % _M
			β2 = (β2 - uint32(sum.head-sum.tail)*αPop + β1) % _M
			β = β1 + _M*β2
		}

		var blockHash *BlockHash

		// Determine if there is a hash match.
		if hh, ok := library.hashLookup[β]; ok {
			blockHash = findUniqueHash(hh, ctx.uniqueHash(buffer[sum.tail:sum.head]), shortSize)
			if DEBUG_SYNC {
				fmt.Printf("found unique hash? %+v\n", blockHash != nil)
			}
		}
		// Send data off if there is data available and a hash is found (so the buffer before it
		// must be flushed first), or the data chunk size has reached it's maximum size (for buffer
		// allocation purposes) or to flush the end of the data.
		if data.tail < data.head && (blockHash != nil || data.head-data.tail >= MaxDataOp) {
			err = enqueue(Operation{Type: OpData, Data: buffer[data.tail:data.head]})
			if err != nil {
				return err
			}
			data.tail = data.head
		}

		if blockHash != nil {
			err = enqueue(Operation{Type: OpBlock, FileIndex: blockHash.FileIndex, BlockIndex: blockHash.BlockIndex})
			if err != nil {
				return err
			}
			rolling = false
			sum.tail += ctx.blockSize

			// There is prior knowledge that any available data
			// buffered will have already been sent. Thus we can
			// assume data.head and data.tail are the same.
			// May trigger "data wrap".
			data.head = sum.tail
			data.tail = sum.tail
		} else {
			if lastRun {
				err = enqueue(Operation{Type: OpData, Data: buffer[data.tail:validTo]})
				if err != nil {
					return err
				}
			} else {
				// The following is for the next loop iteration, so don't try to calculate if last.
				if rolling {
					αPop = uint32(buffer[sum.tail])
				}
				sum.tail += 1

				// May trigger "data wrap".
				data.head = sum.tail
			}
		}
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
