package simpledb

import (
	"fmt"
	"math/rand"
	"os"
)

func SaveData(name string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", name, rand.Int())
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}
	defer func() { // 4. discard the temporary file if it still exists
		fp.Close() // not expected to fail
		if err != nil {
			os.Remove(tmp)
		}
	}()

	if _, err = fp.Write(data); err != nil { // 1. save to the temporary file
		return err
	}
	if err = fp.Sync(); err != nil { // 2. fsync
		return err
	}
	err = os.Rename(tmp, name) // 3. replace the target
	return err
}
