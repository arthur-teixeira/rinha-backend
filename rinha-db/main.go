package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

const ROW_SIZE = 27

type Transaction struct {
	Valor       int32
	Descricao   [10]byte
	Tipo        byte
	RealizadaEm int64
}

type DBTransaction struct {
  T Transaction
  Id int32
}


type Account struct {
	mu                   sync.RWMutex
	Id                   int8
	Balance              int32
	Limit                int32
	Db                   *Database
}

type File struct {
	data   []byte
	length int64
	dirty  bool
}

type Database struct {
	Capacity int
	File     *File
	row      int
	rowSize  int
	CurrentTransactionId int32
}

func mmapFile(f *os.File, size int) *File {
	err := syscall.Ftruncate(int(f.Fd()), int64(size))
	if err != nil {
		log.Fatalf("ftruncate %s: %v", f.Name(), err)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, (size+4095)&^4095, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		log.Fatalf("mmap %s: %v", f.Name(), err)
	}

	return &File{
		data:   data,
		length: int64(size),
		dirty:  false,
	}
}

var (
	ErrUnmappedMemory  = errors.New("unmapped memory")
	ErrIndexOutOfBound = errors.New("offset out of mapped region")
)

func (m *File) boundaryChecks(offset, numBytes int64) error {
	if m.data == nil {
		return ErrUnmappedMemory
	} else if offset+numBytes > m.length || offset < 0 {
		return ErrIndexOutOfBound
	}

	return nil
}
func (m *File) ReadAt(dest []byte, offset int64) (int, error) {
	err := m.boundaryChecks(offset, 1)
	if err != nil {
		return 0, err
	}

	return copy(dest, m.data[offset:]), nil
}

func (m *File) WriteAt(src []byte, offset int64) (int, error) {
	err := m.boundaryChecks(offset, 1)
	if err != nil {
		return 0, err
	}

	m.dirty = true
	return copy(m.data[offset:], src), nil
}

func (m *File) Flush(flags int) error {
	if !m.dirty {
		return nil
	}

	_, _, err := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&m.data[0])), uintptr(m.length), uintptr(flags))
	if err != 0 {
		return err
	}

	m.dirty = false
	return nil
}

func (d *Database) Insert(t Transaction) error {
	position := int64((d.row % d.Capacity) * d.rowSize)

	var buf []byte
	b := bytes.NewBuffer(buf)

	size := binary.Size(t)

	err := binary.Write(b, binary.BigEndian, t)
	if err != nil {
		return err
	}

  binary.Write(b, binary.BigEndian, d.CurrentTransactionId)

	n, err := d.File.WriteAt(b.Bytes(), position)
	if n != size + binary.Size(d.CurrentTransactionId) {
		panic("Error writing transaction")
	}

	if err != nil {
		return err
	}

	d.row += 1
  d.CurrentTransactionId += 1

	return nil
}

func (d *Database) GetTransactions() (*[]DBTransaction, error) {
	var transactions []DBTransaction

	limit := int(math.Min(float64(d.Capacity), float64(d.row)))
	for row := 0; row < limit; row++ {
		row_content := d.File.data[row*d.rowSize : (row+1)*d.rowSize]
    t := Transaction{
			Valor:       int32(binary.BigEndian.Uint32(row_content[0:4])),
			Descricao:   [10]byte(row_content[4:14]),
			Tipo:        row_content[14],
			RealizadaEm: int64(binary.BigEndian.Uint64(row_content[15:23])),
		}

		transactions = append(transactions, DBTransaction{
      T: t,
      Id: int32(binary.BigEndian.Uint32(row_content[23:27])),
    })
	}

	return &transactions, nil
}

func InitDatabase(filePath string, capacity int, rowSize int) *Database {
	f, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("Error creating file: %s", err)
	}

	file := mmapFile(f, capacity*rowSize)
	if file == nil {
		log.Fatalf("Error memory mapping file")
	}

	if err != nil {
		log.Fatalf("Error reading file: %s", err)
	}

	return &Database{Capacity: capacity, File: file, row: 0, rowSize: rowSize}
}

func (a *Account) PerformTransaction(t Transaction) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if t.Tipo == 'c' {
		a.Balance += t.Valor
	} else {
		a.Balance -= t.Valor
		if a.Balance < -a.Limit {
			a.Balance += t.Valor
			return errors.New("ENOBALANCE")
		}
	}
	return a.Db.Insert(t)
}

func (a *Account) GetTransactions() (*[]Transaction, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

  dbTransactions, err := a.Db.GetTransactions()
  if err != nil {
    return nil, err
  }

  sort.SliceStable(*dbTransactions, func (i, j int) bool {
    return (*dbTransactions)[i].Id > (*dbTransactions)[j].Id
  })

  var result []Transaction
  for _, t := range *dbTransactions {
    result = append(result, t.T)
  }

  return &result, nil
}

func InitAccount(id int8, limit int32) *Account {
	db := InitDatabase(fmt.Sprintf("./account_%d.rinha", id), 10, ROW_SIZE)

	return &Account{
		Id:      id,
		Db:      db,
		Balance: 0,
		Limit:   limit,
	}
}

func main() {
	fmt.Println("Starting server...")

	accounts := make(map[int8]*Account)
	accounts[1] = InitAccount(1, 100_000)
	accounts[2] = InitAccount(2, 80_000)
	accounts[3] = InitAccount(3, 1_000_000)
	accounts[4] = InitAccount(4, 10_000_000)
	accounts[5] = InitAccount(5, 500_000)

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
			if err.Error() == "EOF" {
				continue
			}
			fmt.Printf("Error reading message: %s", err.Error())
		}

		method := buf[0]
		accountId := buf[1]
		account, ok := accounts[int8(accountId)]

		switch method {
		case 'i':
			if !ok {
				fmt.Printf("Account not found: %d\n", accountId)
				return
			}
			err = insertTransaction(conn, buf[2:], account)
		case 'g':
			if !ok {
				fmt.Printf("Account not found: %d\n", accountId)
				return
			}
			err = getTransactions(conn, account)
		default:
			return
		}

		if err != nil {
			fmt.Printf("Error writing message: %s\n", err.Error())
			return
		}
	}
}

func getTransactions(conn net.Conn, account *Account) error {
	transactions, err := account.GetTransactions()
	if err != nil {
		return err
	}

	responseSize := int32(binary.Size(transactions))

	err = binary.Write(conn, binary.BigEndian, responseSize)
	if err != nil {
		return err
	}

	err = binary.Write(conn, binary.BigEndian, account.Balance)
	if err != nil {
		return err
	}

	return binary.Write(conn, binary.BigEndian, transactions)
}

func insertTransaction(conn net.Conn, buf []byte, account *Account) error {
	transactionType := buf[0]
	amount := binary.BigEndian.Uint32(buf[1:5])
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

	success := 0

	err := account.PerformTransaction(transaction)
	if err != nil {
		if err.Error() == "ENOBALANCE" {
			success = 1
		} else {
			return err
		}
	}

	err = binary.Write(conn, binary.BigEndian, int8(success))
	if err != nil {
		return err
	}

	return binary.Write(conn, binary.BigEndian, account.Balance)
}
