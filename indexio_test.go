package indexio

import (
	"os"
	"testing"
)

func TestMain(t *testing.T) {
	var (
		i   *Indexio
		idx uint32
		err error
	)

	if i, err = New(".test_data"); err != nil {
		return
	}
	defer os.RemoveAll(".test_data")
	defer i.Close()

	if idx, err = i.Next("foo"); err != nil {
		t.Fatal(err)
	} else if idx != 0 {
		t.Fatalf("Expected %d and received %d", 0, idx)
	}

	if idx, err = i.Next("foo"); err != nil {
		t.Fatal(err)
	} else if idx != 1 {
		t.Fatalf("Expected %d and received %d", 0, idx)
	}

	if idx, err = i.Next("foo"); err != nil {
		t.Fatal(err)
	} else if idx != 2 {
		t.Fatalf("Expected %d and received %d", 0, idx)
	}

	if idx, err = i.Next("foo"); err != nil {
		t.Fatal(err)
	} else if idx != 3 {
		t.Fatalf("Expected %d and received %d", 0, idx)
	}

	if idx, err = i.Current("foo"); err != nil {
		t.Fatal(err)
	} else if idx != 3 {
		t.Fatalf("Expected %d and received %d", 0, idx)
	}

	if err = i.Close(); err != nil {
		t.Fatal(err)
	}

	if i, err = New(".test_data"); err != nil {
		return
	}

	if idx, err = i.Current("foo"); err != nil {
		t.Fatal(err)
	} else if idx != 3 {
		t.Fatalf("Expected %d and received %d", 0, idx)
	}
}

func TestMain64(t *testing.T) {
	var (
		i   *Indexio64
		idx uint64
		err error
	)

	if i, err = New64(".test_data"); err != nil {
		return
	}
	defer os.RemoveAll(".test_data")
	defer i.Close()

	if idx, err = i.Next("foo"); err != nil {
		t.Fatal(err)
	} else if idx != 0 {
		t.Fatalf("Expected %d and received %d", 0, idx)
	}

	if idx, err = i.Next("foo"); err != nil {
		t.Fatal(err)
	} else if idx != 1 {
		t.Fatalf("Expected %d and received %d", 0, idx)
	}

	if idx, err = i.Next("foo"); err != nil {
		t.Fatal(err)
	} else if idx != 2 {
		t.Fatalf("Expected %d and received %d", 0, idx)
	}

	if idx, err = i.Next("foo"); err != nil {
		t.Fatal(err)
	} else if idx != 3 {
		t.Fatalf("Expected %d and received %d", 0, idx)
	}

	if idx, err = i.Current("foo"); err != nil {
		t.Fatal(err)
	} else if idx != 3 {
		t.Fatalf("Expected %d and received %d", 0, idx)
	}

	if err = i.Close(); err != nil {
		t.Fatal(err)
	}

	if i, err = New64(".test_data"); err != nil {
		return
	}

	if idx, err = i.Current("foo"); err != nil {
		t.Fatal(err)
	} else if idx != 3 {
		t.Fatalf("Expected %d and received %d", 0, idx)
	}
}
