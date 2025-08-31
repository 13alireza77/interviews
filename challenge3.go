package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type Event struct {
	Type      string
	Data      interface{}
	Timestamp time.Time
}

type EventHandler interface {
	Handle(ctx context.Context, event Event) error
}

type EventBus struct {
	handlers map[string][]EventHandler
	mu       sync.RWMutex
}

func NewEventBus() *EventBus {
	return &EventBus{
		handlers: make(map[string][]EventHandler),
	}
}

func (eb *EventBus) Subscribe(eventType string, handler EventHandler) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.handlers[eventType] = append(eb.handlers[eventType], handler)
}

func (eb *EventBus) PublishAsync(ctx context.Context, event Event) {
	go func() {
		if err := eb.publish(ctx, event); err != nil {
			log.Printf("Error publishing event %s: %v", event.Type, err)
		}
	}()
}

func (eb *EventBus) publish(ctx context.Context, event Event) error {
	eb.mu.RLock()
	handlers := eb.handlers[event.Type]
	eb.mu.RUnlock()

	if len(handlers) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errors := make(chan error, len(handlers))

	for _, handler := range handlers {
		wg.Add(1)
		go func(h EventHandler) {
			defer wg.Done()
			if err := h.Handle(ctx, event); err != nil {
				errors <- fmt.Errorf("handler failed: %w", err)
			}
		}(handler)
	}

	wg.Wait()
	close(errors)

	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("some handlers failed: %v", errs)
	}

	return nil
}

type Course struct {
	ID          string
	Title       string
	Description string
	Instructor  string
	CreatedAt   time.Time
}

type CourseCreatedEvent struct {
	Course Course
}

type CourseService struct {
	eventBus *EventBus
}

func NewCourseService(eventBus *EventBus) *CourseService {
	return &CourseService{
		eventBus: eventBus,
	}
}

func (cs *CourseService) CreateCourse(ctx context.Context, title, description, instructor string) (*Course, error) {
	course := &Course{
		ID:          fmt.Sprintf("course_%d", time.Now().UnixNano()),
		Title:       title,
		Description: description,
		Instructor:  instructor,
		CreatedAt:   time.Now(),
	}

	log.Printf("Course created: %s", course.ID)

	event := Event{
		Type:      "course.created",
		Data:      CourseCreatedEvent{Course: *course},
		Timestamp: time.Now(),
	}

	cs.eventBus.PublishAsync(ctx, event)

	return course, nil
}

type EmailNotificationHandler struct{}

func (h *EmailNotificationHandler) Handle(ctx context.Context, event Event) error {
	courseEvent := event.Data.(CourseCreatedEvent)

	log.Printf("Sending notification email for course: %s", courseEvent.Course.Title)
	time.Sleep(100 * time.Millisecond)

	log.Printf("Email sent successfully for course: %s", courseEvent.Course.Title)
	return nil
}

type DashboardUpdateHandler struct{}

func (h *DashboardUpdateHandler) Handle(ctx context.Context, event Event) error {
	courseEvent := event.Data.(CourseCreatedEvent)

	log.Printf("Updating admin dashboard for course: %s", courseEvent.Course.Title)
	time.Sleep(50 * time.Millisecond)

	log.Printf("Dashboard updated for course: %s", courseEvent.Course.Title)
	return nil
}

type SearchIndexHandler struct{}

func (h *SearchIndexHandler) Handle(ctx context.Context, event Event) error {
	courseEvent := event.Data.(CourseCreatedEvent)

	log.Printf("Indexing course in search system: %s", courseEvent.Course.Title)
	time.Sleep(200 * time.Millisecond)

	log.Printf("Course indexed successfully: %s", courseEvent.Course.Title)
	return nil
}
