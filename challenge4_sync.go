package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type UserData struct {
	ID        string
	Name      string
	Email     string
	UpdatedAt time.Time
}

type UpdateMessage struct {
	ID         string
	UserData   UserData
	Timestamp  time.Time
	RetryCount int
}

type MessageQueue struct {
	messages []UpdateMessage
	mu       sync.Mutex
}

func NewMessageQueue() *MessageQueue {
	return &MessageQueue{
		messages: make([]UpdateMessage, 0),
	}
}

func (mq *MessageQueue) Enqueue(message UpdateMessage) {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	mq.messages = append(mq.messages, message)
	log.Printf("Message queued: %s (retry: %d)", message.UserData.ID, message.RetryCount)
}

func (mq *MessageQueue) Dequeue() (UpdateMessage, bool) {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	if len(mq.messages) == 0 {
		return UpdateMessage{}, false
	}

	message := mq.messages[0]
	mq.messages = mq.messages[1:]

	return message, true
}

func (mq *MessageQueue) Size() int {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	return len(mq.messages)
}

type Service struct {
	Name     string
	IsDown   bool
	mu       sync.RWMutex
	userData map[string]UserData
}

func NewService(name string) *Service {
	return &Service{
		Name:     name,
		userData: make(map[string]UserData),
	}
}

func (s *Service) UpdateUser(ctx context.Context, userData UserData) error {
	s.mu.RLock()
	isDown := s.IsDown
	s.mu.RUnlock()

	if isDown {
		return fmt.Errorf("service %s is down", s.Name)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(50 * time.Millisecond):
	}

	s.mu.Lock()
	s.userData[userData.ID] = userData
	s.mu.Unlock()

	log.Printf("%s: User %s updated successfully", s.Name, userData.ID)
	return nil
}

func (s *Service) SetDown(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.IsDown = down
	if down {
		log.Printf("%s: Service is now down", s.Name)
	} else {
		log.Printf("%s: Service is now up", s.Name)
	}
}

func (s *Service) GetUserData(userID string) (UserData, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, exists := s.userData[userID]
	return data, exists
}

type SyncManager struct {
	queue    *MessageQueue
	services map[string]*Service
	config   SyncConfig
}

type SyncConfig struct {
	MaxRetries    int
	RetryDelay    time.Duration
	MaxRetryDelay time.Duration
	BackoffFactor float64
}

func NewSyncManager(config SyncConfig) *SyncManager {
	return &SyncManager{
		queue:    NewMessageQueue(),
		services: make(map[string]*Service),
		config:   config,
	}
}

func (sm *SyncManager) RegisterService(service *Service) {
	sm.services[service.Name] = service
}

func (sm *SyncManager) UpdateUserData(ctx context.Context, userData UserData) {
	message := UpdateMessage{
		ID:         fmt.Sprintf("msg_%d", time.Now().UnixNano()),
		UserData:   userData,
		Timestamp:  time.Now(),
		RetryCount: 0,
	}

	sm.queue.Enqueue(message)
}

func (sm *SyncManager) StartSyncWorker(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sm.processQueue(ctx)
			}
		}
	}()
}

func (sm *SyncManager) processQueue(ctx context.Context) {
	for {
		message, exists := sm.queue.Dequeue()
		if !exists {
			break
		}

		sm.processMessage(ctx, message)
	}
}

func (sm *SyncManager) processMessage(ctx context.Context, message UpdateMessage) {
	var failedServices []string

	for serviceName, service := range sm.services {
		if err := service.UpdateUser(ctx, message.UserData); err != nil {
			failedServices = append(failedServices, serviceName)
			log.Printf("Failed to update %s: %v", serviceName, err)
		}
	}

	if len(failedServices) > 0 && message.RetryCount < sm.config.MaxRetries {
		delay := time.Duration(float64(sm.config.RetryDelay) *
			sm.config.BackoffFactor * float64(message.RetryCount+1))

		if delay > sm.config.MaxRetryDelay {
			delay = sm.config.MaxRetryDelay
		}

		retryMessage := message
		retryMessage.RetryCount++
		retryMessage.Timestamp = time.Now()

		log.Printf("Re-queuing message for retry %d in %v (failed: %v)",
			retryMessage.RetryCount, delay, failedServices)

		go func() {
			time.Sleep(delay)
			sm.queue.Enqueue(retryMessage)
		}()
	} else if len(failedServices) > 0 {
		log.Printf("Message failed permanently after %d retries (failed: %v)",
			message.RetryCount, failedServices)
	} else {
		log.Printf("Message delivered successfully to all services")
	}
}

func (sm *SyncManager) GetSyncStatus() map[string]interface{} {
	return map[string]interface{}{
		"queue_size": sm.queue.Size(),
		"services":   len(sm.services),
	}
}
