package lib

import (
	"fmt"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type EndpointWorkerPool struct {
	name string

	workers []*EndpointWorker
	mu      sync.Mutex

	minWorkers int
	maxWorkers int

	queue chan *Request
	stop  chan struct{}
}

var endpointPools map[string]*EndpointWorkerPool
var manager *QueueManager
var bufferSize = 5000

func GetCurrentTotalWorkers() int {
	var total int
	for _, pool := range endpointPools {
		total += len(pool.workers)
	}
	return total
}

func InitWorkerPool(qManager *QueueManager) {
	manager = qManager

	endpointPools = map[string]*EndpointWorkerPool{
		"POST /channels/!/messages/bulk-delete": {
			name:       "POST /channels/!/messages/bulk-delete",
			minWorkers: 3,
			maxWorkers: 20,
			queue:      make(chan *Request, bufferSize),
			stop:       make(chan struct{}),
			workers:    make([]*EndpointWorker, 0),
		},
		"DELETE /channels/!/messages/!": {
			name:       "DELETE /channels/!/messages/!",
			minWorkers: 5,
			maxWorkers: 15,
			queue:      make(chan *Request, bufferSize),
			stop:       make(chan struct{}),
			workers:    make([]*EndpointWorker, 0),
		},
		"GET /channels/!/messages": {
			name:       "GET /channels/!/messages",
			minWorkers: 3,
			maxWorkers: 30,
			queue:      make(chan *Request, bufferSize),
			stop:       make(chan struct{}),
			workers:    make([]*EndpointWorker, 0),
		},
		"GET /guilds/!/members": {
			name:       "GET /guilds/!/members",
			minWorkers: 2,
			maxWorkers: 5,
			queue:      make(chan *Request, bufferSize),
			stop:       make(chan struct{}),
			workers:    make([]*EndpointWorker, 0),
		},
		"GET /guilds/!/members/!": {
			name:       "GET /guilds/!/members/!",
			minWorkers: 2,
			maxWorkers: 5,
			queue:      make(chan *Request, bufferSize),
			stop:       make(chan struct{}),
			workers:    make([]*EndpointWorker, 0),
		},
		"FALLBACK": {
			name:       "FALLBACK",
			minWorkers: 3,
			maxWorkers: 25,
			queue:      make(chan *Request, bufferSize),
			stop:       make(chan struct{}),
			workers:    make([]*EndpointWorker, 0),
		},
	}

	for _, pool := range endpointPools {
		for i := 0; i < pool.minWorkers; i++ {
			worker := CreateEndpointWorker(pool, i)

			pool.workers = append(pool.workers, worker)
			go Worker(worker, pool)
		}
	}

	ticker := time.NewTicker(1 * time.Second)

	go func() {
		for range ticker.C {
			for poolname, pool := range endpointPools {
				for _, worker := range pool.workers {
					status := 0
					if worker.busy {
						status = 1
					}
					WorkerStatus.WithLabelValues(poolname, fmt.Sprint(worker.id)).Set(float64(status))
				}

				WorkerPoolQueueLength.WithLabelValues(poolname).Set(float64(len(pool.queue)))
			}
		}
	}()
}

func CreateRabbitMQChannel(conn *amqp091.Connection) *amqp091.Channel {
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	return ch
}

var startingWorkers = 0

func CreateEndpointWorker(pool *EndpointWorkerPool, id int) *EndpointWorker {
	startingWorkers++

	return &EndpointWorker{
		id:           id,
		busy:         false,
		closed:       false,
		requestQueue: make(chan *Request),
		manager:      manager,
	}
}

func AssignRequestToWorker(pool *EndpointWorkerPool, poolname string, request *Request) {
	pool.mu.Lock()
	for workerId, worker := range pool.workers {
		worker.mu.Lock()
		if !worker.busy && !worker.closed {

			logger.Debugf("Assigning request in %v to worker %v", poolname, workerId)

			worker.busy = true
			worker.mu.Unlock()
			pool.mu.Unlock()

			select {
			case worker.requestQueue <- request:
				return
			default:
				logger.Warnf("Worker %v requestQueue full or closed", workerId)
				return
			}
		}
		worker.mu.Unlock()
	}
	pool.mu.Unlock()

	if len(pool.workers) < pool.maxWorkers || GetCurrentTotalWorkers() < 50 {
		worker := CreateEndpointWorker(pool, len(pool.workers))

		worker.mu.Lock()
		worker.busy = true
		worker.mu.Unlock()

		pool.mu.Lock()
		pool.workers = append(pool.workers, worker)
		pool.mu.Unlock()

		go Worker(worker, pool)

		select {
		case worker.requestQueue <- request:
			return
		default:
			logger.Warnf("New worker requestQueue closed or full immediately")
			return
		}
	}

	pool.queue <- request

}

func SortAndDispatchMessage(channel *amqp091.Channel, msg amqp091.Delivery) {
	request := NewRequest(channel, msg)
	if request == nil {
		logger.Errorf("Failed to create request from message")
		return
	}

	poolPath := GetMetricsPath(request.URL.Path)
	if poolPath == "" {
		logger.Warnf("Failed to get pool path for %v", request.URL.Path)
		return
	}

	poolPath = request.Method + " " + poolPath

	var routedPool string
	pool, ok := endpointPools[poolPath]
	if !ok {
		pool = endpointPools["FALLBACK"]
		routedPool = "FALLBACK"
	} else {
		routedPool = poolPath
	}

	logger.Debugf("Dispatching request %v to %v", poolPath, routedPool)

	AssignRequestToWorker(pool, routedPool, request)
}
