package consistenthash

import (
	"hash/crc32"
	"sort"
)

type HashFunc func(data []byte) uint32

type NodeMap struct {
	hashFunc    HashFunc
	nodeHashs   []int
	nodehashMap map[int]string // 记录节点在哪
}

func NewNodeMap(fn HashFunc) *NodeMap {
	// fn 自定义哈希函数
	m := &NodeMap{
		hashFunc:    fn,
		nodehashMap: make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

func (m *NodeMap) IsEmpty() bool {
	return len(m.nodeHashs) == 0
}

func (m *NodeMap) AddNode(keys ...string) {
	for _, key := range keys {
		if key == " " {
			continue
		}
		hash := int(m.hashFunc([]byte(key)))
		m.nodeHashs = append(m.nodeHashs, hash)
		m.nodehashMap[hash] = key
	}
	sort.Ints(m.nodeHashs) // 对map表格中的key进行排序
}

func (m *NodeMap) PickNode(key string) string {
	if m.IsEmpty() {
		return " "
	}
	hash := int(m.hashFunc([]byte(key)))
	idx := sort.Search(len(m.nodeHashs), func(i int) bool {
		return m.nodeHashs[i] >= hash
	})
	if idx == len(m.nodeHashs) {
		idx = 0
	}
	return m.nodehashMap[m.nodeHashs[idx]] // 找节点名称
}
