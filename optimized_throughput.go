package main

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

type Job struct {
	ID       int
	Duration time.Duration
	CPU      int
}

type Metric struct {
	NodeID    int
	Used      int
	Capacity  int
	Timestamp time.Time
}

type Node struct {
	ID       int
	Capacity int
	Used     int
	JobChan  chan Job
	Stop     chan struct{}
	mu       sync.Mutex
}

func NewNode(id, cap int) *Node {
	n := &Node{ID: id, Capacity: cap, JobChan: make(chan Job, 50), Stop: make(chan struct{})}
	go n.run()
	return n
}

func (n *Node) run() {
	for {
		select {
		case j, ok := <-n.JobChan:
			if !ok {
				return
			}
			n.mu.Lock()
			n.Used += j.CPU
			n.mu.Unlock()
			time.Sleep(j.Duration)
			n.mu.Lock()
			n.Used -= j.CPU
			n.mu.Unlock()
		case <-n.Stop:
			return
		}
	}
}

type ResourceManager struct {
	Nodes         []*Node
	JobQueue      chan Job
	MetricsStream chan Metric
	DecisionChan  chan int
	stop          chan struct{}
	window        map[int][]int
	mu            sync.Mutex
}

func NewResourceManager(nodes []*Node) *ResourceManager {
	rm := &ResourceManager{
		Nodes:         nodes,
		JobQueue:      make(chan Job, 200),
		MetricsStream: make(chan Metric, 500),
		DecisionChan:  make(chan int, 200),
		stop:          make(chan struct{}),
		window:        make(map[int][]int),
	}
	for _, n := range nodes {
		rm.window[n.ID] = []int{}
	}
	go rm.collectMetrics()
	go rm.optimizer()
	go rm.dispatcher()
	return rm
}

func (rm *ResourceManager) collectMetrics() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-rm.stop:
			return
		case m := <-rm.MetricsStream:
			rm.mu.Lock()
			w := rm.window[m.NodeID]
			w = append(w, m.Used)
			if len(w) > 10 {
				w = w[len(w)-10:]
			}
			rm.window[m.NodeID] = w
			rm.mu.Unlock()
		case <-ticker.C:
			for _, n := range rm.Nodes {
				n.mu.Lock()
				u := n.Used
				c := n.Capacity
				n.mu.Unlock()
				rm.MetricsStream <- Metric{NodeID: n.ID, Used: u, Capacity: c, Timestamp: time.Now()}
			}
		}
	}
}

func (rm *ResourceManager) predictLoad(nodeID int) float64 {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	w := rm.window[nodeID]
	if len(w) == 0 {
		return 0
	}
	sum := 0
	for _, v := range w {
		sum += v
	}
	return float64(sum) / float64(len(w))
}

func (rm *ResourceManager) scoreNode(n *Node) float64 {
	load := rm.predictLoad(n.ID)
	avail := float64(n.Capacity) - load
	if avail < 0 {
		avail = 0
	}
	latencyFactor := 1.0 - math.Min(load/float64(n.Capacity), 1.0)
	return avail*0.7 + latencyFactor*float64(n.Capacity)*0.3
}

func (rm *ResourceManager) optimizer() {
	for {
		select {
		case <-rm.stop:
			return
		case job := <-rm.JobQueue:
			best := -1
			bestScore := -1.0
			for _, n := range rm.Nodes {
				n.mu.Lock()
				if n.Used+job.CPU <= n.Capacity {
					score := rm.scoreNode(n)
					if score > bestScore {
						bestScore = score
						best = n.ID
					}
				}
				n.mu.Unlock()
			}
			if best >= 0 {
				rm.DecisionChan <- best
				go func(j Job, nid int) {
					for _, n := range rm.Nodes {
						if n.ID == nid {
							n.JobChan <- j
							return
						}
					}
				}(job, best)
			} else {
				go func(j Job) {
					time.Sleep(300 * time.Millisecond)
					rm.JobQueue <- j
				}(job)
			}
		}
	}
}

func (rm *ResourceManager) dispatcher() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-rm.stop:
			return
		case <-ticker.C:
			fmt.Println("---- Decision Snapshot ----")
			for _, n := range rm.Nodes {
				n.mu.Lock()
				util := float64(n.Used) / float64(n.Capacity) * 100
				n.mu.Unlock()
				fmt.Printf("Node-%d Util: %.1f%%\n", n.ID, util)
			}
			fmt.Println("---------------------------")
		}
	}
}

func spawnJobs(rm *ResourceManager, count int) {
	for i := 1; i <= count; i++ {
		j := Job{ID: i, Duration: time.Duration(200+rand.Intn(800)) * time.Millisecond, CPU: 1 + rand.Intn(3)}
		rm.JobQueue <- j
		time.Sleep(80 * time.Millisecond)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	nodes := []*Node{NewNode(1, 8), NewNode(2, 8), NewNode(3, 8)}
	rm := NewResourceManager(nodes)
	for _, n := range nodes {
		go func(nd *Node) {
			for {
				time.Sleep(1 * time.Second)
				nd.mu.Lock()
				used := nd.Used
				cap := nd.Capacity
				nd.mu.Unlock()
				rm.MetricsStream <- Metric{NodeID: nd.ID, Used: used, Capacity: cap, Timestamp: time.Now()}
			}
		}(n)
	}
	go spawnJobs(rm, 60)
	time.Sleep(20 * time.Second)
	close(rm.stop)
	for _, n := range nodes {
		close(n.Stop)
		close(n.JobChan)
	}
}
