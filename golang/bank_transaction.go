package main

import (
	"fmt"
	"sync"
	"time"
)

type Account struct {
	ID      int
	Balance int
	mu      sync.Mutex
}

type Transaction interface {
	Execute(*Account) error
}

type Deposit struct {
	Amount int
}

func (d Deposit) Execute(a *Account) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Balance += d.Amount
	return nil
}

type Withdrawal struct {
	Amount int
}

func (w Withdrawal) Execute(a *Account) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.Balance < w.Amount {
		return fmt.Errorf("insufficient funds")
	}
	a.Balance -= w.Amount
	return nil
}

func ProcessTransaction(a *Account, t Transaction, resultChan chan<- error) {
	err := t.Execute(a)
	resultChan <- err
}

func main() {
	account := &Account{ID: 1, Balance: 1000}
	resultChan := make(chan error, 2)

	fmt.Printf("Initial balance: $%d\n", account.Balance)

	// Start goroutines for concurrent transactions
	go ProcessTransaction(account, Deposit{Amount: 500}, resultChan)
	go ProcessTransaction(account, Withdrawal{Amount: 700}, resultChan)

	for i := 0; i < 2; i++ {
		if err := <-resultChan; err != nil {
			fmt.Printf("Transaction error: %v\n", err)
		}
	}

	defer fmt.Printf("Final balance: $%d\n", account.Balance)

	// Simulate some processing time
	time.Sleep(time.Second)

	fmt.Println("Transactions completed")
}
