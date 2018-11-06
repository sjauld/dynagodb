package dynagodb

import (
	"log"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Backend provides a connection to dynamo
type Backend struct {
	Service   dynamodbiface.DynamoDBAPI
	TableName *string
}

// New returns a pointer to a Backend
func New(t string) *Backend {
	sess := session.New()
	return &Backend{
		Service:   dynamodb.New(sess),
		TableName: &t,
	}
}

// RetrieveAll returns a pointer to an Iter
func (b *Backend) RetrieveAll() *Iter {
	return NewIter(b)
}

// PutItem stores your go object in the table
func (b *Backend) PutItem(v interface{}) error {
	item, err := dynamodbattribute.MarshalMap(v)
	if err != nil {
		return err
	}

	log.Printf("[DEBUG] item: %+v", item)

	_, err = b.Service.PutItem(&dynamodb.PutItemInput{
		TableName: b.TableName,
		Item:      item,
	})
	return err
}
