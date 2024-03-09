package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

type Transaction struct {
	Valor       int32
	Descricao   [10]byte
	Tipo        byte
	RealizadaEm int64
}

type Account struct {
	mu      sync.Mutex
	Id      int8
	Balance int32
	Db      *Database
}

type Database struct {
	Capacity int
	File     *os.File
	row      int
	rowSize  int
}

func (d *Database) IncreaseRowCount() error {
	d.row = (d.row + 1) % d.Capacity
	_, err := d.File.Seek(0, 0)
	if err != nil {
		return err
	}
	_, err = d.File.Write([]byte{byte(d.row)})
	return err
}

func (d *Database) GetPosition() int64 {
  const rowCountOffset = 1
  return  int64(d.row*d.rowSize) + rowCountOffset
}

func (d *Database) Insert(t Transaction) error {
  position := d.GetPosition()
	_, err := d.File.Seek(position, 0)
	if err != nil {
		return err
	}

	err = binary.Write(d.File, binary.BigEndian, t)
	if err != nil {
		return err
	}

	err = d.File.Sync()
	if err != nil {
		return err
	}

	return d.IncreaseRowCount()
}

func (d *Database) GetTransaction(row int) (*Transaction, error) {
	position := d.GetPosition()
	_, err := d.File.Seek(position, 0)
	if err != nil {
		return nil, err
	}

	var t Transaction
	err = binary.Read(d.File, binary.BigEndian, &t)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

func (d *Database) GetTransactions() (*[]Transaction, error) {
	var transactions []Transaction
	for i := 0; i < d.row; i++ {
		t, err := d.GetTransaction(i)
		if err != nil {
			return nil, err
		}

		transactions = append(transactions, *t)
	}

	return &transactions, nil
}

func InitDatabase(filePath string, capacity int, rowSize int) *Database {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Fatalf("Error opening file: %s", err)
	}

	rowCount := make([]byte, 1)
	_, err = file.Read(rowCount)
	if err != nil {
		if err.Error() == "EOF" {
			rowCount[0] = 0
			_, err = file.Write([]byte{0})
			if err != nil {
				log.Fatalf("Error reading file: %s", err)
			}
		}

		_, err = file.Seek(1, 0)
		if err != nil {
			log.Fatalf("Error reading file: %s", err)
		}
	}

	if err != nil {
		log.Fatalf("Error reading file: %s", err)
	}

	return &Database{Capacity: capacity, File: file, row: int(rowCount[0]), rowSize: rowSize}
}

func (a *Account) PerformTransaction(t Transaction) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if t.Tipo == 'c' {
		a.Balance += t.Valor
	} else {
		a.Balance -= t.Valor
	}

	return a.Db.Insert(t)
}

func (a *Account) GetTransactions() (*[]Transaction, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.Db.GetTransactions()

}

func InitAccount(id int8) *Account {
	// 15 bytes : 1 byte para o tipo, 4 para o valor, 10 para a descrição
	db := InitDatabase(fmt.Sprintf("./account_%d.rinha", id), 10, 15)

	return &Account{Id: id, Db: db, Balance: 0}
}

func main() {
	fmt.Println("Starting server...")

	accounts := make(map[int8]*Account)
	accounts[1] = InitAccount(1)
	accounts[2] = InitAccount(2)
	accounts[3] = InitAccount(3)
	accounts[4] = InitAccount(4)
	accounts[5] = InitAccount(5)

	listener, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %s", err.Error())
			continue
		}

		go handleConnection(conn, accounts)
	}
}

func handleConnection(conn net.Conn, accounts map[int8]*Account) {
	defer conn.Close()

	for {
		buf := make([]byte, 50)
		_, err := conn.Read(buf)
		if err != nil {
			fmt.Printf("Error reading message: %s", err.Error())
		}

		method := buf[0]
		accountId := buf[1] - '0'
		account, ok := accounts[int8(accountId)]

		switch method {
		case 'i':
			if !ok {
				log.Fatal("Account not found")
			}
			err = insertTransaction(conn, buf[2:], account)
		case 'g':
			if !ok {
				log.Fatal("Account not found")
			}
			err = getTransactions(conn, account)
		default:
			return
		}

		if err != nil {
			fmt.Printf("Error writing message: %s\n", err.Error())
		}

	}
}

func getTransactions(conn net.Conn, account *Account) error {
	err := binary.Write(conn, binary.BigEndian, account.Balance)
	if err != nil {
		return err
	}

	transactions, err := account.GetTransactions()
	if err != nil {
		return err
	}

	err = binary.Write(conn, binary.BigEndian, transactions)
	if err != nil {
		return err
	}

	return nil
}

func insertTransaction(conn net.Conn, buf []byte, account *Account) error {
	transactionType := buf[0]
	amount := binary.LittleEndian.Uint32(buf[1:5])
	description := buf[5:]
	if len(description) < 10 {
		description = append(description, make([]byte, 10-len(description))...)
	}

	transaction := Transaction{
		Valor:       int32(amount),
		Descricao:   [10]byte(description),
		Tipo:        transactionType,
		RealizadaEm: time.Now().Unix(),
	}

	err := account.PerformTransaction(transaction)
	if err != nil {
		return err
	}

	return binary.Write(conn, binary.BigEndian, account.Balance)
}
