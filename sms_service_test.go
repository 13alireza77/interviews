package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

type MockSMSProvider struct {
	shouldFail bool
	delay      time.Duration
	callCount  int
}

func (m *MockSMSProvider) SendSMS(ctx context.Context, req SMSRequest) (SMSResponse, error) {
	m.callCount++

	if m.delay > 0 {
		select {
		case <-ctx.Done():
			return SMSResponse{}, ctx.Err()
		case <-time.After(m.delay):
		}
	}

	if m.shouldFail {
		return SMSResponse{}, &SMSError{
			Type:    "MockError",
			Message: "Mock provider failure",
			Err:     errors.New("mock failure"),
		}
	}

	return SMSResponse{
		MessageID: "mock_msg_123",
		Status:    "sent",
	}, nil
}

func TestRobustSMSService_Success(t *testing.T) {
	primary := &MockSMSProvider{shouldFail: false}
	fallback := &MockSMSProvider{shouldFail: false}

	service := NewRobustSMSService(primary, fallback)

	req := SMSRequest{
		PhoneNumber: "+1234567890",
		Message:     "Test message",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := service.SendSMS(ctx, req)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if response.MessageID != "mock_msg_123" {
		t.Errorf("Expected message ID 'mock_msg_123', got: %s", response.MessageID)
	}

	if primary.callCount != 1 {
		t.Errorf("Expected primary provider to be called once, got: %d", primary.callCount)
	}

	if fallback.callCount != 0 {
		t.Errorf("Expected fallback provider to not be called, got: %d", fallback.callCount)
	}
}

func TestRobustSMSService_PrimaryFailure_FallbackSuccess(t *testing.T) {
	primary := &MockSMSProvider{shouldFail: true}
	fallback := &MockSMSProvider{shouldFail: false}

	service := NewRobustSMSService(primary, fallback)

	req := SMSRequest{
		PhoneNumber: "+1234567890",
		Message:     "Test message",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := service.SendSMS(ctx, req)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if response.Status != "sent" {
		t.Errorf("Expected status 'sent', got: %s", response.Status)
	}

	if primary.callCount < 1 {
		t.Errorf("Expected primary provider to be called at least once, got: %d", primary.callCount)
	}

	if fallback.callCount != 1 {
		t.Errorf("Expected fallback provider to be called once, got: %d", fallback.callCount)
	}
}

func TestRobustSMSService_AllProvidersFail(t *testing.T) {
	primary := &MockSMSProvider{shouldFail: true}
	fallback := &MockSMSProvider{shouldFail: true}

	service := NewRobustSMSService(primary, fallback)

	req := SMSRequest{
		PhoneNumber: "+1234567890",
		Message:     "Test message",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := service.SendSMS(ctx, req)

	if err == nil {
		t.Error("Expected error, got nil")
	}

	if response.MessageID != "" {
		t.Errorf("Expected empty message ID, got: %s", response.MessageID)
	}

	if smsErr, ok := err.(*SMSError); ok {
		if smsErr.Type != "AllProvidersFailed" {
			t.Errorf("Expected error type 'AllProvidersFailed', got: %s", smsErr.Type)
		}
	} else {
		t.Error("Expected SMSError type")
	}
}

func TestRobustSMSService_Timeout(t *testing.T) {
	primary := &MockSMSProvider{shouldFail: false, delay: 2 * time.Second}
	fallback := &MockSMSProvider{shouldFail: false, delay: 2 * time.Second}

	service := NewRobustSMSService(primary, fallback)

	req := SMSRequest{
		PhoneNumber: "+1234567890",
		Message:     "Test message",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	response, err := service.SendSMS(ctx, req)

	if err == nil {
		t.Error("Expected timeout error, got nil")
	}

	if response.MessageID != "" {
		t.Errorf("Expected empty message ID, got: %s", response.MessageID)
	}
}

func TestCircuitBreaker_StateTransitions(t *testing.T) {
	cb := NewCircuitBreaker(3, 1*time.Second)

	if !cb.canExecute() {
		t.Error("Circuit breaker should be closed initially")
	}

	for i := 0; i < 3; i++ {
		cb.recordFailure()
	}

	if cb.canExecute() {
		t.Error("Circuit breaker should be open after threshold failures")
	}

	time.Sleep(1100 * time.Millisecond)

	if !cb.canExecute() {
		t.Error("Circuit breaker should be half-open after timeout")
	}

	cb.recordSuccess()

	if !cb.canExecute() {
		t.Error("Circuit breaker should be closed after success")
	}
}
