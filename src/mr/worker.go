package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	CallExample(mapf, reducef)

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.askTask = true
	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)

	if ok {
		if reply.work == Map {
			filename := reply.filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			for _, item := range kva {
				partitionIndex := ihash(item.Key)
				intermediateFileName := getIntermediateFileName(reply.mapIndex, partitionIndex)
				writeToIntermediate(intermediateFileName, item)
			}
		}

	} else {
		fmt.Printf("call failed!\n")
	}
}

func getIntermediateFileName(mapIndex string, partitionIndex int) string {
	return "inter-" + mapIndex + "-" + string(partitionIndex)
}

func writeToIntermediate(filename string, item KeyValue) error {

	var _, err = os.Stat(filename)
	var ofile *os.File
	if os.IsNotExist(err) {
		ofile, err = os.Create(filename)
		if err != nil {
			fmt.Println(err)
			return err
		}

	} else {
		ofile, err = os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return err
		}
	}

	_, err = fmt.Fprintf(ofile, "%v %v\n", item.Key, item.Value)

	if err != nil {
		log.Fatalf("cannot write to %v", filename)
		return err
	}

	return ofile.Close()
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
