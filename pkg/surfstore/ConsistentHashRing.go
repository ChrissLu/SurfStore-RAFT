package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	//blockHash := c.Hash(blockId)

	hashes := make([]string, 0)
	for h := range c.ServerMap {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)
	ResponsibleServer := ""
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			ResponsibleServer = c.ServerMap[hashes[i]]
			break
		}
	}
	if ResponsibleServer == "" {
		ResponsibleServer = c.ServerMap[hashes[0]]
	}

	return ResponsibleServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	HashRing := &ConsistentHashRing{ServerMap: make(map[string]string)}
	for _, serverName := range serverAddrs {
		serverHash := HashRing.Hash("blockstore" + serverName)
		HashRing.ServerMap[serverHash] = serverName
	}

	return HashRing
}
