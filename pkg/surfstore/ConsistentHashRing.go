package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string // key: hashcode, value: server address
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	hashes := make([]string, 0)
	for hash := range c.ServerMap {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	for _, hash := range hashes {
		if blockId < hash {
			return c.ServerMap[hash]
		}
	}
	return c.ServerMap[hashes[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	serverMap := make(map[string]string)
	rst := &ConsistentHashRing{}
	for _, serverAdd := range serverAddrs {
		hash := rst.Hash("blockstore" + serverAdd)
		serverMap[hash] = serverAdd
	}
	rst.ServerMap = serverMap
	return rst
}
