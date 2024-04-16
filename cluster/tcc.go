package cluster

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"reids-by-go/database"
	"reids-by-go/lib/timewheel"
	"sync"
	"time"
)

type Transaction struct {
	id       string
	cmdLines []database.CmdLine
	cluster  *Discovery
	db       *database.DB

	writeKeys  []string
	readKeys   []string
	keysLocked bool
	undoLog    []database.CmdLine

	status int8
	mu     *sync.Mutex
}

const (
	maxLockTime       = 3 * time.Second
	waitBeforeCleanTx = 2 * maxLockTime

	createdStatus    = 0
	preparedStatus   = 1
	committedStatus  = 2
	rolledBackStatus = 3
)

func (t *Transaction) lockKeys() {
	if !t.keysLocked {
		t.db.RWLock(t.writeKeys, t.readKeys)
		t.keysLocked = true
	}
}

func (t *Transaction) unlockKeys() {
	if t.keysLocked {
		t.db.RWUnLock(t.writeKeys, t.readKeys)
		t.keysLocked = false
	}
}

func NewTransaction(cmdLines []database.CmdLine, discovery *Discovery, config clientv3.Config) *Transaction {
	fnv64 := hashFnv32(discovery.self)
	worker, err := NewWorker(fnv64)
	if err != nil {
		return nil
	}
	return &Transaction{
		id:         worker.GetId(),
		cmdLines:   cmdLines,
		cluster:    discovery,
		writeKeys:  make([]string, 0),
		readKeys:   make([]string, 0),
		keysLocked: false,
		undoLog:    make([]database.CmdLine, 0),
		status:     0,
		mu:         &sync.Mutex{},
	}
}

func genTaskKey(txID string) string {
	return "tx:" + txID
}

func (t *Transaction) Prepare() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 锁定相关 key 避免并发问题
	t.writeKeys, t.readKeys = GetRelatedKeys(t.cmdLines, t.db, t.cluster)
	t.lockKeys()

	//准备 undoLog
	t.undoLog = t.db.GetUndoLogs(t.cmdLines)
	t.status = preparedStatus
	//在时间轮中添加任务, 自动回滚超时未提交的事务
	taskKey := genTaskKey(t.id)
	timewheel.Delay(maxLockTime, taskKey, func() {
		if t.status == preparedStatus {
			log.Println("abort transaction: " + t.id)
			t.mu.Lock()
			defer t.mu.Unlock()
			_ = t.RollBack()
		}
	})
	return nil
}

func GetRelatedKeys(cmdLines []database.CmdLine, db *database.DB, discovery *Discovery) ([]string, []string) {
	writeKeys, readKeys := make([]string, 0), make([]string, 0)
	for i := 0; i < len(cmdLines); i++ {
		get := discovery.ConsistentHash.Get(string(cmdLines[i][1]))
		if get == discovery.self {
			if IsWrite(cmdLines[i]) {
				writeKeys = append(writeKeys, get)
			} else {
				readKeys = append(readKeys, get)
			}
		}
	}
	return writeKeys, readKeys
}

func (t *Transaction) RollBack() error {
	for i := 0; i < len(t.undoLog); i++ {
		t.db.Exec(t.undoLog[i])
	}
	return nil
}

func IsWrite(cmdline database.CmdLine) bool {
	cmd := string(cmdline[0])
	return database.CmdIsWriteMap[cmd]
}
