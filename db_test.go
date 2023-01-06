package zDb

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// 测试开启数据库实例
func TestOpen(t *testing.T) {
	db, err := Open("/tmp/zdb")
	if err != nil {
		t.Error(err)
	}
	t.Log(db)
}

// 测试数据写入put
func TestZdbPut(t *testing.T) {
	db, err := Open("/tmp/zdb")
	if err != nil {
		t.Error(err)
	}

	rand.Seed(time.Now().UnixNano())
	keyPrefix := "test_key_"
	valPrefix := "test_val_"
	for i := 0; i < 1000; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i%5))
		value := []byte(valPrefix + strconv.FormatInt(rand.Int63(), 10))
		err = db.Put(key, value)
	}
	if err != nil {
		t.Log(err)
	}
}

// 测试数据读取Get
func TestZDb_Get(t *testing.T) {
	db, err := Open("/tmp/zdb")
	if err != nil {
		t.Error(err)
	}
	getVal := func(key []byte) {
		val, err := db.Get(key)
		if err != nil {
			t.Error("read val error:", err)
		} else {
			if val != nil {
				t.Logf("key = %s, val = %s\n", key, val)
			} else {
				t.Logf("key = %s is not existed!", key)
			}
		}
	}
	getVal([]byte("test_key_0"))
	getVal([]byte("test_key_1"))
	getVal([]byte("test_key_2"))
	getVal([]byte("test_key_3"))
	getVal([]byte("test_key_4"))
	// 不存在，应报错
	getVal([]byte("test_key_5"))
}

func TestZDb_Del(t *testing.T) {
	db, err := Open("/tmp/zdb")
	if err != nil {
		t.Error(err)
	}

	key := []byte("test_key_4")
	err = db.Del(key)

	if err != nil {
		t.Error("del err:", err)
	}
}

func TestZDb_Merge(t *testing.T) {
	db, err := Open("/tmp/zdb")
	if err != nil {
		t.Error(err)
	}
	err = db.Merge()
	if err != nil {
		t.Error("merge error:", err)
	}
}
