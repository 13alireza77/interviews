package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

type MockEventHandler struct {
	callCount int
	mu        sync.Mutex
}

func (h *MockEventHandler) Handle(ctx context.Context, event Event) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.callCount++
	return nil
}

func (h *MockEventHandler) GetCallCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.callCount
}

func TestEventBus_MultipleHandlers(t *testing.T) {
	eventBus := NewEventBus()
	handler1 := &MockEventHandler{}
	handler2 := &MockEventHandler{}

	eventBus.Subscribe("test.event", handler1)
	eventBus.Subscribe("test.event", handler2)

	event := Event{
		Type:      "test.event",
		Data:      "test data",
		Timestamp: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	eventBus.PublishAsync(ctx, event)

	time.Sleep(100 * time.Millisecond)

	if handler1.GetCallCount() != 1 {
		t.Errorf("Expected handler1 to be called once, got: %d", handler1.GetCallCount())
	}

	if handler2.GetCallCount() != 1 {
		t.Errorf("Expected handler2 to be called once, got: %d", handler2.GetCallCount())
	}
}
