package badger

import (
	"testing"

	badgerlib "github.com/dgraph-io/badger/v4"
	"github.com/magiconair/properties"
)

func TestGetScanIteratorOptionsDefault(t *testing.T) {
	p := properties.NewProperties()

	opts, err := getScanIteratorOptions(p)
	if err != nil {
		t.Fatalf("getScanIteratorOptions returned error: %v", err)
	}
	if opts.PrefetchSize != badgerlib.DefaultIteratorOptions.PrefetchSize {
		t.Fatalf("PrefetchSize = %d, want %d", opts.PrefetchSize, badgerlib.DefaultIteratorOptions.PrefetchSize)
	}
	if opts.PrefetchValues != badgerlib.DefaultIteratorOptions.PrefetchValues {
		t.Fatalf("PrefetchValues = %v, want %v", opts.PrefetchValues, badgerlib.DefaultIteratorOptions.PrefetchValues)
	}
}

func TestGetScanIteratorOptionsOverride(t *testing.T) {
	p := properties.NewProperties()
	p.Set(badgerScanPrefetchSize, "32")

	opts, err := getScanIteratorOptions(p)
	if err != nil {
		t.Fatalf("getScanIteratorOptions returned error: %v", err)
	}
	if opts.PrefetchSize != 32 {
		t.Fatalf("PrefetchSize = %d, want 32", opts.PrefetchSize)
	}
}

func TestGetScanIteratorOptionsRejectsNonPositive(t *testing.T) {
	p := properties.NewProperties()
	p.Set(badgerScanPrefetchSize, "0")

	if _, err := getScanIteratorOptions(p); err == nil {
		t.Fatal("expected error for non-positive prefetch size")
	}
}
