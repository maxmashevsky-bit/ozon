package memory_test

import (
	"example.com/ozon/internal/core"
	"example.com/ozon/internal/store/contract"
	"example.com/ozon/internal/store/memory"
	"testing"
)

func TestContract(t *testing.T) {
	contract.Run(t, func(t *testing.T) core.Store { return memory.New() })
}

func TestSubscriptions(t *testing.T) { store := memory.New(); contract.Subscriptions(t, store, store) }
