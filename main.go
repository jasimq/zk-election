package main

import (
	"flag"
	"log"
	"strconv"
	"strings"
	"time"

	zk "github.com/samuel/go-zookeeper/zk"
)

var zkHost *string = flag.String("zookeeper", "localhost:2181", "Zookeeper host ")

func main() {
	createZkNode("election")

	//Enter election and wait for turn
	election()

	//Elected. Do work
}

func election() (err error) {
	z, _, err := zk.Connect([]string{*zkHost}, time.Second)

	if err != nil {
		panic(err)
	}

	// Don't close the connection as that will kill the ephemeral node
	//defer z.Close()

	// STEP 0: Register self and get a guid
	path := "/election/"
	guid, err := z.CreateProtectedEphemeralSequential(path, []byte{}, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Printf("Error creating EphemeralSequential node: %s", err)
		panic(err)
	}

	log.Printf("GUID: %s", guid)
	seq, _ := strconv.ParseInt(guid[strings.LastIndex(guid, "-")+1:], 10, 16)

	path = "/election"

	for {
		// STEP 1: Watch all children
		data, _, channel, err := z.ChildrenW(path)
		if err != nil {
			log.Printf("Error watching node: %s", err)
			panic(err)
		}

		//		log.Printf("Children: %+v", data)

		// If this is the first client then it becomes the leader
		if len(data) == 1 {
			log.Printf("First one to the party!")
			break
		}

		//block here and listen for events
		log.Printf("Waiting on channel...")
		event := <-channel

		if event.Type == zk.EventNodeChildrenChanged {
			//			log.Printf("EVENT: %+v", event)

			// Get all the children and check who has the smallest guid
			children, _, err := z.Children(path)
			if err != nil {
				log.Printf("Error getting children: %s", err)
				panic(err)
			}

			// If this is the only child left then it becomes the leader
			if len(children) == 1 {
				log.Printf("Only child left.")
				break
			}

			var smallest int64 = 0
			for _, child := range children {
				childSeq, _ := strconv.ParseInt(child[strings.LastIndex(child, "-")+1:], 10, 16)

				if smallest == 0 || childSeq < smallest {
					smallest = childSeq
					log.Printf("SMALLEST %s: %s", guid, smallest)
				}
			}

			// If the current guid is the smallest, then current process becomes
			// the new leader and we can break out of this election loop
			if seq <= smallest {
				log.Printf("Seq %d elected leader!", seq)
				break
			}
		}
	}

	log.Printf("Election ended %s", guid)
	return err
}

func createZkNode(path string) {
	z, _, err := zk.Connect([]string{*zkHost}, time.Second)
	defer z.Close()

	if err != nil {
		panic(err)
	}

	nodes := strings.Split(path, "/")
	toCreate := ""
	for i := 0; i < len(nodes); i++ {
		toCreate = toCreate + "/" + nodes[i]

		if exists, _, _ := z.Exists(toCreate); !exists {
			if _, err = z.Create(toCreate, []byte{}, 0, zk.WorldACL(zk.PermAll)); err != nil {
				panic(err)
			}
		}
		log.Printf("toCreate: %s", toCreate)
	}

}
