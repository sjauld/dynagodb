package dynagodb

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
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

// GetItem retrieves a go object from the table, using a consistent read
func (b *Backend) GetItem(v interface{}, key map[string]interface{}) error {
	var primaryKey = make(map[string]*dynamodb.AttributeValue)
	for k, val := range key {
		valAttr, err := dynamodbattribute.Marshal(val)
		if err != nil {
			return fmt.Errorf("Could not marshal primary key: %v", err)
		}
		primaryKey[k] = valAttr
	}
	in := &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		Key:            primaryKey,
		TableName:      b.TableName,
	}
	out, err := b.Service.GetItem(in)
	if err != nil {
		return fmt.Errorf("Could not GetItem from DynamoDB: %v", err)
	}

	err = dynamodbattribute.UnmarshalMap(out.Item, v)
	if err != nil {
		return fmt.Errorf("Could not unmarshal: %v", err)
	}

	return nil
}

// PutItem stores your go object in the table
func (b *Backend) PutItem(v interface{}) error {
	item, err := dynamodbattribute.MarshalMap(v)
	if err != nil {
		return err
	}

	_, err = b.Service.PutItem(&dynamodb.PutItemInput{
		Item:      item,
		TableName: b.TableName,
	})
	return err
}

// RetrieveAll returns a pointer to an Iter
func (b *Backend) RetrieveAll() *Iter {
	return NewIter(b)
}
