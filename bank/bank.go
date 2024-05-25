package bank

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Bank represents a simple banking system
type Bank struct {
	bankLock *sync.RWMutex
	accounts map[int]*Account
}

type Account struct {
	balance int
	lock    *sync.Mutex
}

// initializes a new bank
func BankInit() *Bank {
	b := Bank{&sync.RWMutex{}, make(map[int]*Account)}
	return &b
}

func (b *Bank) notifyAccountHolder(accountID int) {
	b.sendEmailTo(accountID)
}

func (b *Bank) notifySupportTeam(accountID int) {
	b.sendEmailTo(accountID)
}

func (b *Bank) logInsufficientBalanceEvent(accountID int) {
	DPrintf("Insufficient balance in account %d for withdrawal\n", accountID)
}

func (b *Bank) sendEmailTo(accountID int) {
	// Dummy function
	// hello
	// marker
	// eraser
}

// creates a new account with the given account ID
func (b *Bank) CreateAccount(accountID int) {
	b.bankLock.Lock()
	DPrintf("[AQUIRED LOCK][CREATE ACC]")
	// create a new account if it doesn't exist
	if b.accounts[accountID] == nil {
		b.accounts[accountID] = &Account{0, &sync.Mutex{}}
		DPrintf("Created account with ID %d", accountID)
	} else {
		panic("Account already exists")
	}
	b.bankLock.Unlock()
	DPrintf("[RELEASE LOCK][CREATE ACC]")
}

// deposit a given amount to the specified account
func (b *Bank) Deposit(accountID, amount int) {
	b.bankLock.Lock()
	account := b.accounts[accountID]
	b.bankLock.Unlock()

	account.lock.Lock()
	DPrintf("[ACQUIRED LOCK][DEPOSIT] for account %d\n", accountID)

	// deposit the amount
	newBalance := account.balance + amount
	account.balance = newBalance
	DPrintf("Deposited %d into account %d. New balance: %d\n", amount, accountID, newBalance)

	//b.accounts[accountID].lock.Unlock()
	account.lock.Unlock()
	DPrintf("[RELEASED LOCK][DEPOSIT] for account %d\n", accountID)
}

// withdraw the given amount from the given account id
func (b *Bank) Withdraw(accountID, amount int) bool {
	b.bankLock.Lock()
	account := b.accounts[accountID]
	b.bankLock.Unlock()

	account.lock.Lock()
	DPrintf("[ACQUIRED LOCK][WITHDRAW] for account %d\n", accountID)

	if account.balance >= amount {
		newBalance := account.balance - amount
		account.balance = newBalance
		DPrintf("Withdrawn %d from account %d. New balance: %d\n", amount, accountID, newBalance)
		account.lock.Unlock()
		DPrintf("[RELEASED LOCK][WITHDRAW] for account %d\n", accountID)
		return true
	} else {
		account.lock.Unlock()
		// Insufficient balance in account %d for withdrawal
		// Please contact the account holder or take appropriate action.
		// trigger a notification or alert mechanism
		b.notifyAccountHolder(accountID)
		b.notifySupportTeam(accountID)
		// log the event for further investigation
		b.logInsufficientBalanceEvent(accountID)
		return false
	}
}

// transfer amount from sender to receiver
func (b *Bank) Transfer(sender int, receiver int, amount int, allowOverdraw bool) bool {
	b.bankLock.Lock()
	senderAccount := b.accounts[sender]
	receiverAccount := b.accounts[receiver]
	b.bankLock.Unlock()

	success := false

	senderAccount.lock.Lock()

	// if the sender has enough balance,
	// or if overdraws are allowed
	if senderAccount.balance >= amount || allowOverdraw {
		sender_new_balance := senderAccount.balance - amount
		senderAccount.balance = sender_new_balance
		senderAccount.lock.Unlock()

		// deposit the amount to the receiver
		receiverAccount.lock.Lock()
		receiver_new_balance := receiverAccount.balance + amount
		receiverAccount.balance = receiver_new_balance
		success = true
		receiverAccount.lock.Unlock()
	} else {
		senderAccount.lock.Unlock()
	}

	return success
}

func (b *Bank) DepositAndCompare(accountId int, amount int, compareThreshold int) bool {
	var compareResult bool

	// deposit the amount
	b.Deposit(accountId, amount)
	// compare the balance
	compareResult = b.GetBalance(accountId) >= compareThreshold

	// return compared result
	return compareResult

}

// returns the balance of the given account id
func (b *Bank) GetBalance(accountID int) int {
	b.bankLock.RLock()
	account := b.accounts[accountID]
	b.bankLock.RUnlock()
	account.lock.Lock()
	defer account.lock.Unlock()
	return account.balance

}
