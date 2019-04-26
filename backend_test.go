package dynagodb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	dynamock "github.com/gusaul/go-dynamock"
)

var (
	mock *dynamock.DynaMock
	svc  dynamodbiface.DynamoDBAPI
)

func init() {
	svc, mock = dynamock.New()
}

func testBackend(tableName string) *Backend {
	backend := New(tableName)

	backend.Service = svc
	return backend
}

type MegaTest struct {
	MegaString string
	MegaBool   bool
	MegaInt    int
}

func TestNew(t *testing.T) {
	backend := testBackend("test-table")
	if *backend.TableName != "test-table" {
		t.Errorf("Expected test-table, got %s", *backend.TableName)
	}
}

func mockGetItem() {
	res := dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"MegaString": &dynamodb.AttributeValue{
				S: aws.String("ABC"),
			},
			"MegaInt": &dynamodb.AttributeValue{
				N: aws.String("123"),
			},
			"MegaBool": &dynamodb.AttributeValue{
				BOOL: aws.Bool(true),
			},
		},
	}
	key := map[string]*dynamodb.AttributeValue{
		"MegaString": {
			S: aws.String("ABC"),
		},
	}
	mock.ExpectGetItem().ToTable("test-table").WithKeys(key).WillReturns(res)
}

func TestGetItem(t *testing.T) {
	mockGetItem()
	backend := testBackend("test-table")
	obj := &MegaTest{}

	err := backend.GetItem(obj, map[string]interface{}{"MegaString": "ABC"})
	if err != nil {
		t.Error(err)
	}

	if obj.MegaInt != 123 {
		t.Errorf("Expected 123, got %d", obj.MegaInt)
	}

	if obj.MegaString != "ABC" {
		t.Errorf("Expected ABC, got %v", obj.MegaString)
	}

	if !obj.MegaBool {
		t.Errorf("Expected true, got %t", obj.MegaBool)
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
