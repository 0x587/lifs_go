package main

import "fmt"

func localChunkIndexes(fanout uint32, chunk uint32) []uint32 {
	// 6 is a good guess for max level of pointer chunks;
	// 4MiB chunksize, uint32 chunk index -> 15PiB of data.
	// overflow just means an allocation.
	index := make([]uint32, 0, 6)

	for chunk > 0 {
		index = append(index, chunk%fanout)
		chunk /= fanout
	}
	return index
}

func main() {
	//kv_ := kvmem.NewKvMem()
	//v, _ := volume.NewVolume("/home/szm/test-tmp", kv_)
	//_ = v.Init()
	//_ = v.Scan()
	//fmt.Println(v)
	fmt.Println([]int{1, 2, 3}[1:1])
}
