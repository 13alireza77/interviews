package main

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type Step struct {
	Name     string
	Execute  func(ctx context.Context, data interface{}) error
	Rollback func(ctx context.Context, data interface{}) error
}

type Flow struct {
	steps []Step
	mu    sync.Mutex
}

func NewFlow(steps ...Step) *Flow {
	return &Flow{steps: steps}
}

func (s *Flow) Execute(ctx context.Context, data interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var executedSteps []int
	var lastError error

	for i, step := range s.steps {
		log.Printf("Executing step: %s", step.Name)

		if err := step.Execute(ctx, data); err != nil {
			lastError = fmt.Errorf("step '%s' failed: %w", step.Name, err)
			log.Printf("Step failed: %v", lastError)

			if rollbackErr := s.rollback(ctx, executedSteps, data); rollbackErr != nil {
				log.Printf("Rollback failed: %v", rollbackErr)
				return fmt.Errorf("execution failed: %v, rollback also failed: %v", lastError, rollbackErr)
			}

			return lastError
		}

		executedSteps = append(executedSteps, i)
		log.Printf("Step completed: %s", step.Name)
	}

	log.Printf("All steps completed successfully")
	return nil
}

func (s *Flow) rollback(ctx context.Context, executedSteps []int, data interface{}) error {
	log.Printf("Starting rollback for %d steps", len(executedSteps))

	var rollbackErrors []error

	for i := len(executedSteps) - 1; i >= 0; i-- {
		stepIndex := executedSteps[i]
		step := s.steps[stepIndex]

		log.Printf("Rolling back step: %s", step.Name)

		if step.Rollback != nil {
			if err := step.Rollback(ctx, data); err != nil {
				rollbackErr := fmt.Errorf("rollback for step '%s' failed: %w", step.Name, err)
				log.Printf("Rollback error: %v", rollbackErr)
				rollbackErrors = append(rollbackErrors, rollbackErr)
			} else {
				log.Printf("Rollback completed: %s", step.Name)
			}
		}
	}

	if len(rollbackErrors) > 0 {
		return fmt.Errorf("rollback completed with errors: %v", rollbackErrors)
	}

	return nil
}

type OrderData struct {
	OrderID     string
	CustomerID  string
	ProductID   string
	Quantity    int
	Amount      float64
	OrderStatus string
}

type OrderService struct{}

func (o *OrderService) CreateOrder(_ context.Context, data *OrderData) error {
	data.OrderStatus = "created"
	log.Printf("Order created: %s", data.OrderID)
	return nil
}

func (o *OrderService) CancelOrder(_ context.Context, data *OrderData) error {
	data.OrderStatus = "cancelled"
	log.Printf("Order cancelled: %s", data.OrderID)
	return nil
}

type InventoryService struct{}

func (i *InventoryService) DeductInventory(_ context.Context, data *OrderData) error {
	log.Printf("Inventory deducted for product: %s, quantity: %d", data.ProductID, data.Quantity)
	return nil
}

func (i *InventoryService) RestoreInventory(_ context.Context, data *OrderData) error {
	log.Printf("Inventory restored for product: %s, quantity: %d", data.ProductID, data.Quantity)
	return nil
}

type PaymentService struct{}

func (p *PaymentService) ProcessPayment(_ context.Context, data *OrderData) error {
	log.Printf("Payment processed: $%.2f", data.Amount)
	return nil
}

func (p *PaymentService) RefundPayment(_ context.Context, data *OrderData) error {
	log.Printf("Payment refunded: $%.2f", data.Amount)
	return nil
}

func CreateOrderFlow(orderSvc *OrderService, inventorySvc *InventoryService, paymentSvc *PaymentService) *Flow {
	return NewFlow(
		Step{
			Name: "Create Order",
			Execute: func(ctx context.Context, data interface{}) error {
				orderData := data.(*OrderData)
				return orderSvc.CreateOrder(ctx, orderData)
			},
			Rollback: func(ctx context.Context, data interface{}) error {
				orderData := data.(*OrderData)
				return orderSvc.CancelOrder(ctx, orderData)
			},
		},
		Step{
			Name: "Deduct Inventory",
			Execute: func(ctx context.Context, data interface{}) error {
				orderData := data.(*OrderData)
				return inventorySvc.DeductInventory(ctx, orderData)
			},
			Rollback: func(ctx context.Context, data interface{}) error {
				orderData := data.(*OrderData)
				return inventorySvc.RestoreInventory(ctx, orderData)
			},
		},
		Step{
			Name: "Process Payment",
			Execute: func(ctx context.Context, data interface{}) error {
				orderData := data.(*OrderData)
				return paymentSvc.ProcessPayment(ctx, orderData)
			},
			Rollback: func(ctx context.Context, data interface{}) error {
				orderData := data.(*OrderData)
				return paymentSvc.RefundPayment(ctx, orderData)
			},
		},
	)
}
