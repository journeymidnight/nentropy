package badger

import (
	"fmt"
	"github.com/journeymidnight/badger/table"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var F_Delete = true
var F_Gc = true

func (db *DB) disableDeleteFile() {
	if db.gcTables == nil {
		db.gcTables = make([]*table.Table, 0)
	}
	F_Delete = false
}

func (db *DB) enableDeleteFile() error {
	F_Delete = true
	return decrRefs(db.gcTables)
}

func (db *DB) disableVlogGc() {
	F_Gc = false
	for {
		if db.vlogGcIng == false {
			break
		}
		db.elog.Printf("wait for vlog Gc quit")
		time.Sleep(time.Second)
	}
}

func (db *DB) enableVlogGc() {
	F_Gc = true
}

func closeAllFiles(files []*os.File) {
	for _, file := range files {
		file.Close()
	}
}

func (db *DB) getAliveFiles() ([]*os.File, error) {
	retFiles := make([]*os.File, 0)

	manifest := filepath.Join(db.opt.Dir, ManifestFilename)
	fd, err := os.OpenFile(manifest, os.O_RDONLY, 0666)
	if err != nil {
		fmt.Printf("can not create new manifest")
		db.elog.Errorf("open new manifest file failed")
		return nil, err
	}
	defer fd.Close()
	//copy manifest file and open it
	newPath := filepath.Join(db.opt.Dir, ManifestBackupFilename)
	bfd, err := os.OpenFile(newPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Printf("can not create new manifest")
		db.elog.Errorf("open new manifest file failed")
		return nil, err
	}
	db.manifest.appendLock.Lock()

	_, copy_err := io.Copy(bfd, fd)
	if copy_err != nil {
		db.manifest.appendLock.Unlock()
		db.elog.Errorf("getAliveFiles copy manifest file content failed")
		closeAllFiles(retFiles)
		return nil, err
	}
	bfd.Sync()
	db.manifest.appendLock.Unlock()
	retFiles = append(retFiles, bfd)

	//get all sst fds
	idMap := getIDMap(db.opt.Dir)
	for id := range idMap {
		filename := table.NewFilename(id, db.opt.Dir)
		sstFd, err := os.OpenFile(filename, os.O_RDONLY, 644)
		if err != nil {
			db.elog.Errorf("open new manifest file failed")
			closeAllFiles(retFiles)
			return nil, err
		}
		retFiles = append(retFiles, sstFd)
	}

	//get all vlog fds
	files, err := ioutil.ReadDir(db.vlog.dirPath)
	if err != nil {
		closeAllFiles(retFiles)
		return nil, errors.Wrapf(err, "Error while opening value log")
	}
	found := make(map[uint64]struct{})
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".vlog") {
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.ParseUint(file.Name()[:fsz-5], 10, 32)
		if err != nil {
			closeAllFiles(retFiles)
			return nil, errors.Wrapf(err, "Error while parsing value log id for file: %q", file.Name())
		}
		if _, ok := found[fid]; ok {
			closeAllFiles(retFiles)
			return nil, errors.Errorf("Found the same value log file twice: %d", fid)
		}
		found[fid] = struct{}{}
	}
	for fid := range found {
		vpath := db.vlog.fpath(uint32(fid))
		vfd, err := os.OpenFile(vpath, os.O_RDONLY, 0666)
		if err != nil {
			closeAllFiles(retFiles)
			return nil, errors.Wrapf(err, "Unable to open %q as RDONLY.", vpath)
		}
		retFiles = append(retFiles, vfd)
	}
	return retFiles, nil
}

func (db *DB) QuickBackupPrepare() ([]*os.File, error) {
	db.disableVlogGc()
	db.disableDeleteFile()
	return db.getAliveFiles()
}

func (db *DB) QuickBackupDone() error {
	newPath := filepath.Join(db.opt.Dir, ManifestBackupFilename)
	os.Remove(newPath)
	db.enableVlogGc()
	return db.enableDeleteFile()
}
