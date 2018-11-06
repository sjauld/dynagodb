package dynagodb

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var (
	anItem = map[string]*dynamodb.AttributeValue{
		"beer": &dynamodb.AttributeValue{
			S: str("yum"),
		}}

	keyItem = map[string]*dynamodb.AttributeValue{
		"whiskey": &dynamodb.AttributeValue{
			S: str("yum"),
		}}

	singlePageResponse = dynamodb.ScanOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			anItem,
		},
	}

	multiPageResponse = dynamodb.ScanOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			anItem,
			anItem,
			keyItem,
		},
		LastEvaluatedKey: keyItem,
	}
)

func TestIter_singlePage(t *testing.T) {
	b := testBackend("single-page-test-table")

	mock.ExpectScan().Table("single-page-test-table").WillReturns(singlePageResponse)

	it := NewIter(b)
	if it.err != nil {
		t.Error(it.err)
	}

	testNextItemExists(it, t)
	testBeer(it, t)
	testFinished(it, t)
}

func TestIter_multiPage(t *testing.T) {
	b := testBackend("multi-page-test-table")

	mock.ExpectScan().Table("multi-page-test-table").WillReturns(multiPageResponse)
	mock.ExpectScan().Table("multi-page-test-table").WillReturns(singlePageResponse)

	it := NewIter(b)
	if it.err != nil {
		t.Error(it.err)
	}

	testNextItemExists(it, t)
	testBeer(it, t)
	testNextItemExists(it, t)
	testBeer(it, t)
	testNextItemExists(it, t)
	testWhiskey(it, t)

	testNextItemExists(it, t)
	testBeer(it, t)

	testFinished(it, t)
}

func testBeer(it *Iter, t *testing.T) {
	beer := *it.Current()["beer"].S
	if beer != "yum" {
		t.Errorf("Expected beer to be yum, but it was %v", beer)
	}
}

func testFinished(it *Iter, t *testing.T) {
	if it.Next() {
		t.Errorf("Expected the items to be finished, but got one more!")
	}

	if it.Err() != nil {
		t.Errorf("Didn't finish cleanly: %v", it.Err())
	}
}

func testWhiskey(it *Iter, t *testing.T) {
	whiskey := *it.Current()["whiskey"].S
	if whiskey != "yum" {
		t.Errorf("Expected whiskey to be yum, but it was %v", whiskey)
	}
}

func testNextItemExists(it *Iter, t *testing.T) {
	if !it.Next() {
		t.Errorf("Expecting an item, but there was none! %v", it.Err())
	}
}

func str(s string) *string {
	return &s
}
