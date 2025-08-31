package main

import (
	"context"
	"testing"
	"time"
)

func TestSyncManager_UpdateUserData(t *testing.T) {
	config := SyncConfig{
		MaxRetries:    3,
		RetryDelay:    100 * time.Millisecond,
		MaxRetryDelay: 1 * time.Second,
		BackoffFactor: 1.0,
	}

	syncManager := NewSyncManager(config)

	service := NewService("TestService")
	syncManager.RegisterService(service)

	userData := UserData{
		ID:        "user_001",
		Name:      "Test User",
		Email:     "test@example.com",
		UpdatedAt: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	syncManager.StartSyncWorker(ctx)

	syncManager.UpdateUserData(ctx, userData)

	time.Sleep(500 * time.Millisecond)

	if data, exists := service.GetUserData("user_001"); !exists {
		t.Error("Expected user data to be synced")
	} else if data.Email != "test@example.com" {
		t.Errorf("Expected email 'test@example.com', got: %s", data.Email)
	}
}

func TestSyncManager_RetryOnFailure(t *testing.T) {
	config := SyncConfig{
		MaxRetries:    2,
		RetryDelay:    50 * time.Millisecond,
		MaxRetryDelay: 200 * time.Millisecond,
		BackoffFactor: 1.0,
	}

	syncManager := NewSyncManager(config)

	service := NewService("TestService")
	service.SetDown(true)
	syncManager.RegisterService(service)

	userData := UserData{
		ID:        "user_001",
		Name:      "Test User",
		Email:     "test@example.com",
		UpdatedAt: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	syncManager.StartSyncWorker(ctx)

	syncManager.UpdateUserData(ctx, userData)

	time.Sleep(300 * time.Millisecond)

	if _, exists := service.GetUserData("user_001"); exists {
		t.Error("Expected user data to not be synced when service is down")
	}

	time.Sleep(100 * time.Millisecond)
	status := syncManager.GetSyncStatus()
	if status["queue_size"].(int) == 0 {
		t.Error("Expected queue to have messages after failed attempts")
	}
}

func TestSyncManager_RecoveryAfterFailure(t *testing.T) {
	config := SyncConfig{
		MaxRetries:    3,
		RetryDelay:    50 * time.Millisecond,
		MaxRetryDelay: 200 * time.Millisecond,
		BackoffFactor: 1.0,
	}

	syncManager := NewSyncManager(config)

	service := NewService("TestService")
	service.SetDown(true)
	syncManager.RegisterService(service)

	userData := UserData{
		ID:        "user_001",
		Name:      "Test User",
		Email:     "test@example.com",
		UpdatedAt: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	syncManager.StartSyncWorker(ctx)

	syncManager.UpdateUserData(ctx, userData)

	time.Sleep(200 * time.Millisecond)

	service.SetDown(false)

	time.Sleep(500 * time.Millisecond)

	if data, exists := service.GetUserData("user_001"); !exists {
		t.Error("Expected user data to be synced after service recovery")
	} else if data.Email != "test@example.com" {
		t.Errorf("Expected email 'test@example.com', got: %s", data.Email)
	}
}
