package dynagodb

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/gusaul/go-dynamock"
)

var (
	mock *dynamock.DynaMock
	svc  dynamodbiface.DynamoDBAPI
)

func init() {
	svc, mock = dynamock.New()
}

type MegaTest struct {
	MegaString string
	MegaBool   bool
	MegaInt    int
}

func testBackend(tableName string) *Backend {
	backend := New(tableName)

	backend.Service = svc
	return backend
}

func TestNew(t *testing.T) {
	backend := testBackend("test-table")
	if *backend.TableName != "test-table" {
		t.Errorf("Expected test-table, got %s", *backend.TableName)
	}
}

func TestPutItem(t *testing.T) {
	backend := testBackend("test-table")

	obj := MegaTest{"test", true, 3}

	res := dynamodb.PutItemOutput{}
	mock.ExpectPutItem().ToTable("test-table").WillReturns(res)

	err := backend.PutItem(obj)
	if err != nil {
		t.Error(err)
	}
}
