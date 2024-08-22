package simpledb

import (
	"bytes"
	"encoding/binary"
)

const Header = 4

const (
	BtreePageSize   = 4096
	BtreeMaxKeySize = 1000
	BtreeMaxValSize = 3000
)

var NodeMax = Header + 8 + 2 + 4 + BtreeMaxKeySize + BtreeMaxValSize

type NodeType uint16

const (
	InternalNode NodeType = 1 // internal nodes without values
	LeafNode     NodeType = 2 // leaf nodes with values
)

type BTree struct {
	root uint64                      // pointer(a nonzero page number)
	get  func(uint642 uint64) []byte // dereference a pointer
	new  func([]byte) uint64         // allocate a new page
	del  func(uint642 uint64)        // deallocate a page
}

// BNode represents a node structure in a B-tree stored as a byte slice.
// The layout of the BNode in memory is as follows:
//
// General structure of the BNode:
//
// | type | numberOfKeys |   pointers   |   offsets  | key-values | unused |
//
// |  2B  |  2B   | numberOfKeys * 8B   | numberOfKeys * 2B |     ...    |        |
//
//   - type:     2 bytes indicating the type of the node.
//
//   - numberOfKeys:    2 bytes indicating the number of keys in the node.
//
//   - pointers: An array of 64-bit pointers (8 bytes each), one for each key, indicating
//     the next node or value location.
//
//   - offsets:  An array of 16-bit offsets (2 bytes each), one for each key, indicating
//     the position of key-values in the BNode byte slice.
//
//   - key-values: Variable-sized key-value pairs stored consecutively. Each key-value
//     pair's structure is defined below:
//     | klen | vlen | key   | val   |
//     |  2B  |  2B  | ...   | ...   |
//
//   - klen:  2 bytes indicating the length of the key.
//
//   - vlen:  2 bytes indicating the length of the value.
//
//   - key:   Variable length key, with the length specified by klen.
//
//   - val:   Variable length value, with the length specified by vlen.
//
// - unused:  Remaining bytes in the node that are not in use.
type BNode []byte

func (node BNode) nodeType() NodeType {
	return NodeType(binary.LittleEndian.Uint16(node[0:2]))
}

func (node BNode) numberOfKeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype NodeType, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], uint16(btype))
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

func (node BNode) getPtr(idx uint16) uint64 {
	if idx < node.numberOfKeys() {
		panic("idx is less than node.numberOfKeys")
	}
	pos := Header + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	pos := Header + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

func offsetPos(node BNode, idx uint16) uint16 {
	if 1 <= idx && idx <= node.numberOfKeys() {
		panic("idx is less than node.numberOfKeys")
	}
	return Header + 8*node.numberOfKeys() + 2*(idx-1)
}
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	pos := offsetPos(node, idx)
	binary.LittleEndian.PutUint16(node[pos:], offset)
}

// key-values
func (node BNode) kvPos(idx uint16) uint16 {
	if idx <= node.numberOfKeys() {
		panic("idx is less than node.numberOfKeys")
	}
	return Header + 8*node.numberOfKeys() + 2*node.numberOfKeys() + node.getOffset(idx)
}
func (node BNode) getKey(idx uint16) []byte {
	if idx <= node.numberOfKeys() {
		panic("idx is less than node.numberOfKeys")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+uint16(klen):][:vlen]
}

// node size in bytes
func (node BNode) nBytes() uint16 {
	return node.kvPos(node.numberOfKeys())
}

// returns the first kid node whose range intersects the key. (kid[i] <= key)
// TODO: binary search
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.numberOfKeys()
	found := uint16(0)
	// the first key is a copy from the parent node,
	// thus it's always less than or equal to the key.
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// leafInsert adds a new key to a leaf node
func leafInsert(
	newNode BNode,
	oldNode BNode,
	idx uint16,
	key []byte,
	val []byte,
) {
	newNode.setHeader(LeafNode, oldNode.numberOfKeys()+1) // set up the header
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	nodeAppendKV(newNode, idx, 0, key, val)
	nodeAppendRange(newNode, oldNode, idx+1, idx, oldNode.numberOfKeys()-idx)
}

// leafUpdate updates an exists key
func leafUpdate(
	newNode BNode,
	oldNode BNode,
	idx uint16,
	key []byte,
	val []byte,
) {

}

// copy a KV into the position
func nodeAppendKV(
	newNode BNode,
	idx uint16,
	ptr uint64,
	key []byte,
	val []byte,
) {
	// ptrs
	newNode.setPtr(idx, ptr)
	// KVs
	pos := newNode.kvPos(idx)
	binary.LittleEndian.PutUint16(newNode[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(newNode[pos+2:], uint16(len(val)))
	copy(newNode[pos+4:], key)
	copy(newNode[pos+4+uint16(len(key)):], val)
	// the offset of the next key
	newNode.setOffset(idx+1, newNode.getOffset(idx)+4+uint16((len(key)+len(val))))
}

// copy multiple KVs into the position from the old node
func nodeAppendRange(
	new BNode,
	old BNode,
	dstNew uint16,
	srcOld uint16,
	n uint16,
) {
	for i := uint16(0); i < n; i++ {
		srcIdx, dstIdx := srcOld+i, dstNew+i
		new.setPtr(dstIdx, old.getPtr(srcIdx))
		if old.nodeType() == LeafNode {
			key := old.getKey(srcIdx)
			val := old.getVal(srcIdx)
			pos := new.kvPos(dstIdx)
			kvSize := 4 + len(key) + len(val)
			copy(new[pos:], old[old.kvPos(srcIdx):][:kvSize])
		}
		new.setOffset(dstIdx+1, new.getOffset(dstIdx)+4+old.getOffset(srcIdx+1)-old.getOffset(srcIdx))
	}
}

func nodeReplaceKidN(
	tree *BTree,
	newNode BNode,
	oldNode BNode,
	idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	newNode.setHeader(InternalNode, oldNode.numberOfKeys()+inc-1)
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(newNode, idx+uint16(i), tree.new(node), node.getKey(0), nil)
		//                    ^position      ^pointer        ^key            ^val
	}
	nodeAppendRange(newNode, oldNode, idx+inc, idx+1, oldNode.numberOfKeys()-(idx+1))
}

// split a oversized node into 2 so that the 2nd node always fits on a page
func nodeSplit2(left BNode, right BNode, old BNode) {
	// code omitted...
}

// split a node if it's too big. the results are 1~3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nBytes() <= BtreePageSize {
		old = old[:BtreePageSize]
		return 1, [3]BNode{old} // not split
	}
	left := BNode(make([]byte, 2*BtreePageSize)) // might be split later
	right := BNode(make([]byte, BtreePageSize))
	nodeSplit2(left, right, old)
	if left.nBytes() <= BtreePageSize {
		left = left[:BtreePageSize]
		return 2, [3]BNode{left, right} // 2 nodes
	}
	leftleft := BNode(make([]byte, BtreePageSize))
	middle := BNode(make([]byte, BtreePageSize))
	nodeSplit2(leftleft, middle, left)
	if leftleft.nBytes() <= BtreePageSize {
		panic("leftleft is less than BtreePageSize")
	}
	return 3, [3]BNode{leftleft, middle, right} // 3 nodes
}

// insert a KV into a node, the result might be split.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so
	newNode := make(BNode, 2*BtreePageSize)

	// where to insert the key?
	idx := nodeLookupLE(node, key)
	// act depending on the node type
	switch node.nodeType() {
	case LeafNode:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it.
			leafUpdate(newNode, node, idx, key, val)
		} else {
			// insert it after the position.
			leafInsert(newNode, node, idx+1, key, val)
		}
	case InternalNode:
		// internal node, insert it to a kid node.
		nodeInsert(tree, newNode, node, idx, key, val)
	default:
		panic("bad node!")
	}
	return newNode
}

// part of the treeInsert(): KV insertion to an internal node
func nodeInsert(
	tree *BTree,
	newNode BNode,
	node BNode,
	idx uint16,
	key []byte,
	val []byte,
) {
	kptr := node.getPtr(idx)
	// recursive insertion to the kid node
	knode := treeInsert(tree, tree.get(kptr), key, val)
	// split the result
	nsplit, split := nodeSplit3(knode)
	// deallocate the kid node
	tree.del(kptr)
	// update the kid links
	nodeReplaceKidN(tree, newNode, node, idx, split[:nsplit]...)
}
