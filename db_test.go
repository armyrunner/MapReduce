package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestSplitAndMerge(t *testing.T) {
	var srcTotal int
	srcDB, _ := openDatabase("dbFiles/austen.db")
	srcDB.QueryRow(`SELECT COUNT(key) FROM pairs;`).Scan(&srcTotal)
	srcDB.Close()
	t.Logf("Total Rows:  %d\n", srcTotal)

	tempdir := filepath.Join(os.TempDir(), fmt.Sprintf("mapreduce.%d", os.Getpid()))
	os.MkdirAll(tempdir, 0777)
	defer os.RemoveAll(tempdir)

	filePaths, err := splitDatabase("dbFiles/austen.db", tempdir, mapSourceFile, 10)
	if err == nil {
		var splitCount, splitTotal int
		for _, f := range filePaths {
			splitCount = 0
			fDB, err := openDatabase(f)
			if err == nil {
				fDB.QueryRow(`SELECT COUNT(key) FROM pairs;`).Scan(&splitCount)
				fDB.Close()
				t.Logf("%s:  %d\n", f, splitCount)
				splitTotal += splitCount
			} else {
				t.Error(err)
				break
			}
		}
		if splitTotal != srcTotal {
			t.Errorf("Split Total (%d) != Source Total (%d)", splitTotal, srcTotal)
		}
	} else {
		t.Error(err)
		t.FailNow()
	}

	address := "localhost:8080"
	go func() {
		http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
		if err := http.ListenAndServe(address, nil); err != nil {
			t.Errorf("Error in HTTP server [%s] %v", address, err)
			t.FailNow()
		}
	}()

	for i, file := range filePaths {
		_, fn := filepath.Split(file)
		filePaths[i] = "http://" + address + "/data/" + fn
	}
	_, err = mergeDatabases(filePaths, filepath.Join(tempdir, "copyausten.db"), filepath.Join(tempdir, "temp.db"))
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	var copytot int
	copyDB, _ := openDatabase(filepath.Join(tempdir, "copyausten.db"))
	copyDB.QueryRow(`SELECT COUNT(key) FROM pairs;`).Scan(&copytot)
	copyDB.Close()
	t.Logf("Total Rows Copied:  %d\n", copytot)

	if copytot != srcTotal {
		t.Errorf("Copy Total (%d) != Source Total (%d)", copytot, srcTotal)
	}
}
