package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

type MockService struct {
	shouldFail bool
	callCount  int
}

func (m *MockService) Execute(_ context.Context, _ interface{}) error {
	m.callCount++
	if m.shouldFail {
		return errors.New("mock service failure")
	}
	return nil
}

func (m *MockService) Rollback(_ context.Context, _ interface{}) error {
	return nil
}

func TestFlow_SuccessfulExecution(t *testing.T) {
	service1 := &MockService{shouldFail: false}
	service2 := &MockService{shouldFail: false}
	service3 := &MockService{shouldFail: false}

	flow := NewFlow(
		Step{
			Name: "Step 1",
			Execute: func(ctx context.Context, data interface{}) error {
				return service1.Execute(ctx, data)
			},
			Rollback: func(ctx context.Context, data interface{}) error {
				return service1.Rollback(ctx, data)
			},
		},
		Step{
			Name: "Step 2",
			Execute: func(ctx context.Context, data interface{}) error {
				return service2.Execute(ctx, data)
			},
			Rollback: func(ctx context.Context, data interface{}) error {
				return service2.Rollback(ctx, data)
			},
		},
		Step{
			Name: "Step 3",
			Execute: func(ctx context.Context, data interface{}) error {
				return service3.Execute(ctx, data)
			},
			Rollback: func(ctx context.Context, data interface{}) error {
				return service3.Rollback(ctx, data)
			},
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := flow.Execute(ctx, "test data")

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if service1.callCount != 1 {
		t.Errorf("Expected service1 to be called once, got: %d", service1.callCount)
	}

	if service2.callCount != 1 {
		t.Errorf("Expected service2 to be called once, got: %d", service2.callCount)
	}

	if service3.callCount != 1 {
		t.Errorf("Expected service3 to be called once, got: %d", service3.callCount)
	}
}

func TestFlow_FailureWithRollback(t *testing.T) {
	service1 := &MockService{shouldFail: false}
	service2 := &MockService{shouldFail: true}
	service3 := &MockService{shouldFail: false}

	flow := NewFlow(
		Step{
			Name: "Step 1",
			Execute: func(ctx context.Context, data interface{}) error {
				return service1.Execute(ctx, data)
			},
			Rollback: func(ctx context.Context, data interface{}) error {
				return service1.Rollback(ctx, data)
			},
		},
		Step{
			Name: "Step 2",
			Execute: func(ctx context.Context, data interface{}) error {
				return service2.Execute(ctx, data)
			},
			Rollback: func(ctx context.Context, data interface{}) error {
				return service2.Rollback(ctx, data)
			},
		},
		Step{
			Name: "Step 3",
			Execute: func(ctx context.Context, data interface{}) error {
				return service3.Execute(ctx, data)
			},
			Rollback: func(ctx context.Context, data interface{}) error {
				return service3.Rollback(ctx, data)
			},
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := flow.Execute(ctx, "test data")

	if err == nil {
		t.Error("Expected error, got nil")
	}

	if service1.callCount != 1 {
		t.Errorf("Expected service1 to be called once, got: %d", service1.callCount)
	}

	if service2.callCount != 1 {
		t.Errorf("Expected service2 to be called once, got: %d", service2.callCount)
	}

	if service3.callCount != 0 {
		t.Errorf("Expected service3 to not be called, got: %d", service3.callCount)
	}
}

func TestFlow_ContextCancellation(t *testing.T) {
	slowService := &MockService{shouldFail: false}

	flow := NewFlow(
		Step{
			Name: "Slow Step",
			Execute: func(ctx context.Context, data interface{}) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
					return slowService.Execute(ctx, data)
				}
			},
			Rollback: func(ctx context.Context, data interface{}) error {
				return nil
			},
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := flow.Execute(ctx, "test data")

	if err == nil {
		t.Error("Expected context cancellation error, got nil")
	}
}

func TestOrderFlow_Integration(t *testing.T) {
	orderSvc := &OrderService{}
	inventorySvc := &InventoryService{}
	paymentSvc := &PaymentService{}

	flow := CreateOrderFlow(orderSvc, inventorySvc, paymentSvc)

	orderData := &OrderData{
		OrderID:    "TEST-001",
		CustomerID: "CUST-001",
		ProductID:  "PROD-001",
		Quantity:   1,
		Amount:     50.00,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := flow.Execute(ctx, orderData)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if orderData.OrderStatus != "created" {
		t.Errorf("Expected order status 'created', got: %s", orderData.OrderStatus)
	}
}
