package zDb

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type zDb struct {
	indexes map[string]int64 //内存中的索引
	dbFile  *DbFile          //数据文件
	dbPath  string           //数据目录
	rwm     sync.RWMutex
}

// Open 开启一个数据库实例
func Open(path string) (*zDb, error) {
	// 如果数据库目录不存在，新建一个
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	// 加载数据文件
	dbFile, err := NewDbFile(path)
	if err != nil {
		return nil, err
	}

	db := &zDb{
		dbFile:  dbFile,
		indexes: make(map[string]int64),
		dbPath:  path,
	}

	// 加载索引
	db.loadIndexesFromFile()
	return db, nil
}

// 将数据文件中的Entry读到内存
func (db *zDb) loadIndexesFromFile() {
	if db.dbFile == nil {
		return
	}
	var offset int64
	for {
		entry, err := db.dbFile.Read(offset)
		fmt.Println("entry:", entry)
		if err != nil {
			// 读取完毕
			if err == io.EOF {
				break
			}
			return
		}
		// 设置索引状态
		db.indexes[string(entry.Key)] = offset
		// 如果enrty的标记为DEL，在内存中删除该索引
		if entry.Method == DEL {
			// 删除内存中的key
			delete(db.indexes, string(entry.Key))
		}
		offset += entry.GetSize()
	}
	return
}

func (db *zDb) Put(key []byte, value []byte) (err error) {
	if len(key) == 0 {
		return
	}
	db.rwm.Lock()
	defer db.rwm.Unlock()

	offset := db.dbFile.Offset
	// 封装成entry
	entry := NewEntry(key, value, PUT)
	// 追加到数据文件中
	err = db.dbFile.Write(entry)
	// 写到内存
	db.indexes[string(key)] = offset
	return
}

// Get 取数据
func (db *zDb) Get(key []byte) (value []byte, err error) {
	if len(key) == 0 {
		return
	}
	db.rwm.RLock()
	defer db.rwm.RUnlock()

	// 从内存中取出索引信息
	offset, ok := db.indexes[string(key)]
	// key不存在
	if !ok {
		return
	}
	// 从磁盘中读取数据
	var e *Entry
	e, err = db.dbFile.Read(offset)
	if err != nil && err != io.EOF {
		return
	}
	if e != nil {
		value = e.Value
	}
	return
}

// Del 删除数据
func (db *zDb) Del(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.rwm.Lock()
	defer db.rwm.Unlock()
	// 从内存中取出索引信息
	_, ok := db.indexes[string(key)]
	// key不存在，忽略
	if !ok {
		fmt.Printf("key (%s) is not existed!\n", key)
		return
	}
	// 封装成Entry并写入
	e := NewEntry(key, nil, DEL)
	err = db.dbFile.Write(e)
	if err != nil {
		return
	}

	// 删除内存中的key
	delete(db.indexes, string(key))
	return
}

// Merge 合并数据文件
func (db *zDb) Merge() error {
	// 没有数据，忽略
	if db.dbFile.Offset == 0 {
		return nil
	}
	var (
		validEntries []*Entry
		offset       int64
	)
	// 读取数据文件中的 entry
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			// 读取完毕
			if err == io.EOF {
				break
			}
			return err
		}
		// 内存中的索引状态是最新的，直接对比过滤出有效的entry
		if off, ok := db.indexes[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()
	}

	// 对有效的entry进行处理，放到新的临时文件中
	if len(validEntries) > 0 {
		// 新建临时文件
		mergeDbFile, err := NewMergeDbFile(db.dbPath)
		if err != nil {
			return err
		}
		defer os.Remove(mergeDbFile.File.Name())

		// 重新写入有效的 entry
		for _, entry := range validEntries {
			writeOff := mergeDbFile.Offset
			err = mergeDbFile.Write(entry)
			if err != nil {
				return err
			}
			// 更新索引
			db.indexes[string(entry.Key)] = writeOff
		}

		// 获取当前数据文件文件名
		dbFileName := db.dbFile.File.Name()
		// 关闭文件
		db.dbFile.File.Close()
		// 删除旧的数据文件
		os.Remove(dbFileName)

		// 获取merge数据文件名
		mergeFileName := mergeDbFile.File.Name()
		// 关闭文件
		mergeDbFile.File.Close()
		// merge文件更新为新的数据文件
		os.Rename(mergeFileName, db.dbPath+string(filepath.Separator)+FileName)

		db.dbFile = mergeDbFile
	}
	return nil
}
