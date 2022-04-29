package main

import (
	"fmt"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage/disk"
)

func main() {
	store, err := disk.NewDiskStorage("/tmp/test")
	if err != nil {
		fmt.Printf("failed to create store, %v", err)
		panic("")
	}

	err = store.Create("pod", []byte("data"))
	if err != nil {
		fmt.Printf("failed to create file, %v", err)
	}
}
