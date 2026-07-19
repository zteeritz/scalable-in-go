package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// --- worker pool ---

type Job struct {
	ID      int
	Payload string
}

type Result struct {
	JobID  int
	Output string
	Err    error
}

type WorkerPool struct {
	jobs    chan Job
	results chan Result
	wg      sync.WaitGroup
}

func NewWorkerPool(workerCount int) *WorkerPool {
	pool := &WorkerPool{
		jobs:    make(chan Job, 100),
		results: make(chan Result, 100),
	}

	for i := 0; i < workerCount; i++ {
		pool.wg.Add(1)
		go pool.worker(i)
	}

	return pool
}

func (p *WorkerPool) worker(id int) {
	defer p.wg.Done()
	for job := range p.jobs {
		time.Sleep(1 * time.Second)
		p.results <- Result{
			JobID:  job.ID,
			Output: fmt.Sprintf("Job %d processed by worker %d", job.ID, id),
			Err:    nil,
		}
	}
}

func (p *WorkerPool) Submit(job Job) {
	p.jobs <- job
}

func (p *WorkerPool) Close() {
	close(p.jobs)
	p.wg.Wait()
	close(p.results)
}

// --- user repository ---

type User struct {
	ID   int
	Name string
}

type UserRepository interface {
	FindByID(ctx context.Context, id int) (*User, error)
}

type InMemoryUserRepository struct {
	mu    sync.RWMutex
	users map[int]*User
}

func (r *InMemoryUserRepository) FindByID(ctx context.Context, id int) (*User, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if user, ok := r.users[id]; ok {
		return user, nil
	}

	return nil, fmt.Errorf("user not found")
}

// --- service layer ---

type UserService struct {
	repo   UserRepository
	logger *slog.Logger
}

func (s *UserService) FindByID(ctx context.Context, id int) (*User, error) {
	user, err := s.repo.FindByID(ctx, id)
	if err != nil {
		s.logger.Error("failed to find user", "error", err)
		return nil, err
	}

	return user, nil
}

// --- HTTP handler with grateful shutdown handling ---
func userHandler(svc *UserService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		user, err := svc.FindByID(ctx, 1)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "Hello, %s!", user.Name)
	}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	repo := &InMemoryUserRepository{
		users: map[int]*User{
			1: {ID: 1, Name: "Alice"},
		},
	}
	svc := &UserService{repo: repo, logger: logger}

	pool := NewWorkerPool(10)
	go func() {
		for result := range pool.results {
			logger.Info("job done", "job_id", result.JobID, "output", result.Output)
		}
	}()
	for i := range 10 {
		pool.Submit(Job{ID: i, Payload: fmt.Sprintf("task-%d", i)})
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/user", userHandler(svc))

	srv := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("server started")
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("server shutdown failed", "error", err)
	} else {
		pool.Close()
		logger.Info("server exited properly")
	}
}
