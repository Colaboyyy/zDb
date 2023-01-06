package zDb

import (
	"os"
	"path/filepath"
)

const FileName = "zdb.data"
const MergeFileName = "zdb.data.merge"

type DbFile struct {
	File   *os.File
	Offset int64
}

func newFile(fileName string) (*DbFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	return &DbFile{Offset: stat.Size(), File: file}, nil
}

// 创建一个新的数据文件
func NewDbFile(path string) (*DbFile, error) {
	fileName := path + string(filepath.Separator) + FileName
	return newFile(fileName)
}

// 合并时，用于新建数据文件
func NewMergeDbFile(path string) (*DbFile, error) {
	fileName := path + string(filepath.Separator) + MergeFileName
	return newFile(fileName)
}

// 从offset开始读取
func (df *DbFile) Read(offset int64) (e *Entry, err error) {
	buf := make([]byte, entryHeaderSize)
	if _, err = df.File.ReadAt(buf, offset); err != nil {
		return
	}
	if e, err = Decode(buf); err != nil {
		return
	}
	offset += entryHeaderSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}

	offset += int64(e.KeySize)
	if e.ValueSize > 0 {
		value := make([]byte, e.ValueSize)
		if _, err = df.File.ReadAt(value, offset); err != nil {
			return
		}
		e.Value = value
	}
	return
}

// 写入entry
func (df *DbFile) Write(e *Entry) (err error) {
	enc, err := e.Encode()
	if err != nil {
		return
	}
	_, err = df.File.WriteAt(enc, df.Offset)
	df.Offset += e.GetSize()
	return
}
