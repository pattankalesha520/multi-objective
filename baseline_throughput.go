package main
import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)
type Job struct {
	ID       int
	Duration time.Duration
	CPU      int
}
type Node struct {
	ID       int
	Capacity int
	Used     int
	JobChan  chan Job
	mu       sync.Mutex
}
func (n *Node) run() {
	for j := range n.JobChan {
		n.mu.Lock()
		n.Used += j.CPU
		n.mu.Unlock()
		time.Sleep(j.Duration)
		n.mu.Lock()
		n.Used -= j.CPU
		n.mu.Unlock()
	}
}
type Manager struct {
	Nodes    []*Node
	JobQueue chan Job
}
func (m *Manager) schedule() {
	for job := range m.JobQueue {
		assigned := false
		for _, n := range m.Nodes {
			n.mu.Lock()
			if n.Used+job.CPU <= n.Capacity {
				n.JobChan <- job
				assigned = true
				n.mu.Unlock()
				break
			}
			n.mu.Unlock()
		}
		if !assigned {
			time.Sleep(200 * time.Millisecond)
			m.JobQueue <- job
		}
	}
}
func (m *Manager) monitor() {
	for range time.Tick(2 * time.Second) {
		fmt.Println("---- Cluster Snapshot ----")
		for _, n := range m.Nodes {
			n.mu.Lock()
			util := float64(n.Used) / float64(n.Capacity) * 100
			fmt.Printf("Node-%d: %d/%d (%.1f%%)\n", n.ID, n.Used, n.Capacity, util)
			n.mu.Unlock()
		}
	}
}
func main() {
	rand.Seed(time.Now().UnixNano())
	nodes := []*Node{
{ID: 1, Capacity: 8, JobChan: make(chan Job, 5)},
{ID: 2, Capacity: 8, JobChan: make(chan Job, 5)},
{ID: 3, Capacity: 8, JobChan: make(chan Job, 5)},
	}
	for _, n := range nodes {
		go n.run()
	}
	m := &Manager{Nodes: nodes, JobQueue: make(chan Job, 20)}
	go m.schedule()
	go m.monitor()
	for i := 1; i <= 30; i++ {
		m.JobQueue <- Job{ID: i, Duration: time.Duration(300+rand.Intn(700)) * time.Millisecond, CPU: 1 + rand.Intn(3)}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(10 * time.Second)
	close(m.JobQueue)
	for _, n := range nodes {
		close(n.JobChan)
	}
}
