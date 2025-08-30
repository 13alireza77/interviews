package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"sync"
	"time"
)

type SMSError struct {
	Type    string
	Message string
	Err     error
}

func (e SMSError) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}

type SMSRequest struct {
	PhoneNumber string
	Message     string
}

type SMSResponse struct {
	MessageID string
	Status    string
	Error     error
}

type CircuitBreaker struct {
	mu              sync.RWMutex
	state           string // "closed", "open", "half-open"
	failureCount    int
	lastFailureTime time.Time
	threshold       int
	timeout         time.Duration
}

func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:     "closed",
		threshold: threshold,
		timeout:   timeout,
	}
}

func (cb *CircuitBreaker) canExecute() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	switch cb.state {
	case "closed":
		return true
	case "open":
		if time.Since(cb.lastFailureTime) > cb.timeout {
			cb.mu.RUnlock()
			cb.mu.Lock()
			cb.state = "half-open"
			cb.mu.Unlock()
			cb.mu.RLock()
			return true
		}
		return false
	case "half-open":
		return true
	default:
		return false
	}
}

func (cb *CircuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount = 0
	cb.state = "closed"
}

func (cb *CircuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount++
	cb.lastFailureTime = time.Now()

	if cb.failureCount >= cb.threshold {
		cb.state = "open"
	}
}

type RetryConfig struct {
	MaxAttempts   int
	InitialDelay  time.Duration
	MaxDelay      time.Duration
	BackoffFactor float64
}

type SMSProvider interface {
	SendSMS(ctx context.Context, req SMSRequest) (SMSResponse, error)
}

type ExternalSMSProvider struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

func NewExternalSMSProvider(baseURL, apiKey string) *ExternalSMSProvider {
	return &ExternalSMSProvider{
		baseURL: baseURL,
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (p *ExternalSMSProvider) SendSMS(ctx context.Context, req SMSRequest) (SMSResponse, error) {
	select {
	case <-ctx.Done():
		return SMSResponse{}, &SMSError{
			Type:    "Timeout",
			Message: "Request cancelled",
			Err:     ctx.Err(),
		}
	case <-time.After(100 * time.Millisecond):
	}

	if time.Now().UnixNano()%5 == 0 {
		return SMSResponse{}, &SMSError{
			Type:    "ExternalAPIError",
			Message: "External SMS service temporarily unavailable",
			Err:     errors.New("service unavailable"),
		}
	}

	return SMSResponse{
		MessageID: fmt.Sprintf("msg_%d", time.Now().UnixNano()),
		Status:    "sent",
	}, nil
}

type FallbackSMSProvider struct {
	queue []SMSRequest
	mu    sync.Mutex
}

func NewFallbackSMSProvider() *FallbackSMSProvider {
	return &FallbackSMSProvider{
		queue: make([]SMSRequest, 0),
	}
}

func (p *FallbackSMSProvider) SendSMS(ctx context.Context, req SMSRequest) (SMSResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.queue = append(p.queue, req)

	return SMSResponse{
		MessageID: fmt.Sprintf("fallback_%d", time.Now().UnixNano()),
		Status:    "queued",
	}, nil
}

type RobustSMSService struct {
	primaryProvider  SMSProvider
	fallbackProvider SMSProvider
	circuitBreaker   *CircuitBreaker
	retryConfig      RetryConfig
}

func NewRobustSMSService(primary, fallback SMSProvider) *RobustSMSService {
	return &RobustSMSService{
		primaryProvider:  primary,
		fallbackProvider: fallback,
		circuitBreaker:   NewCircuitBreaker(5, 30*time.Second),
		retryConfig: RetryConfig{
			MaxAttempts:   3,
			InitialDelay:  100 * time.Millisecond,
			MaxDelay:      5 * time.Second,
			BackoffFactor: 2.0,
		},
	}
}

func (s *RobustSMSService) SendSMSWithRetry(ctx context.Context, req SMSRequest) (SMSResponse, error) {
	var lastErr error
	delay := s.retryConfig.InitialDelay

	for attempt := 1; attempt <= s.retryConfig.MaxAttempts; attempt++ {
		if !s.circuitBreaker.canExecute() {
			return SMSResponse{}, &SMSError{
				Type:    "CircuitBreakerOpen",
				Message: "Service temporarily unavailable due to repeated failures",
				Err:     lastErr,
			}
		}

		attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)

		response, err := s.primaryProvider.SendSMS(attemptCtx, req)
		cancel()

		if err == nil {
			s.circuitBreaker.recordSuccess()
			return response, nil
		}

		lastErr = err
		s.circuitBreaker.recordFailure()

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			break
		}

		if attempt == s.retryConfig.MaxAttempts {
			break
		}

		select {
		case <-ctx.Done():
			return SMSResponse{}, ctx.Err()
		case <-time.After(delay):
			delay = time.Duration(math.Min(float64(delay)*s.retryConfig.BackoffFactor, float64(s.retryConfig.MaxDelay)))
		}
	}

	return SMSResponse{}, lastErr
}

func (s *RobustSMSService) SendSMS(ctx context.Context, req SMSRequest) (SMSResponse, error) {
	response, err := s.SendSMSWithRetry(ctx, req)
	if err == nil {
		return response, nil
	}

	log.Printf("Primary SMS provider failed: %v", err)

	log.Println("Attempting fallback SMS provider...")
	fallbackCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	fallbackResponse, fallbackErr := s.fallbackProvider.SendSMS(fallbackCtx, req)
	if fallbackErr != nil {
		return SMSResponse{}, &SMSError{
			Type:    "AllProvidersFailed",
			Message: "Both primary and fallback SMS providers are unavailable",
			Err:     fmt.Errorf("primary: %v, fallback: %v", err, fallbackErr),
		}
	}

	return fallbackResponse, nil
}

func (s *RobustSMSService) GetServiceStatus() map[string]interface{} {
	s.circuitBreaker.mu.RLock()
	defer s.circuitBreaker.mu.RUnlock()

	return map[string]interface{}{
		"circuit_breaker_state": s.circuitBreaker.state,
		"failure_count":         s.circuitBreaker.failureCount,
		"last_failure_time":     s.circuitBreaker.lastFailureTime,
		"threshold":             s.circuitBreaker.threshold,
	}
}
