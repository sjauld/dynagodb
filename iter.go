package dynagodb

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Iter provides a convenient interface for iterating over the elements returned
// from paginated list API calls. Successive calls to the Next method will step
// through each item in the list, fetching pages of items as needed. Iterators
// are not thread-safe, so they should not be consumed across multiple
// goroutines.
type Iter struct {
	b            *Backend
	cur          map[string]*dynamodb.AttributeValue
	err          error
	nextStartKey map[string]*dynamodb.AttributeValue
	values       []map[string]*dynamodb.AttributeValue
}

// NewIter returns a pointer to an Iter for the backend provided. It performs
// an initial scan to populate the first page, and stores the next key to scan.
func NewIter(b *Backend) *Iter {
	it := &Iter{
		b: b,
	}

	it.getNextPage()

	return it
}

// Current returns the most recent item visited by a call to Next.
func (it *Iter) Current() map[string]*dynamodb.AttributeValue {
	return it.cur
}

// Err returns the error, if any, that caused the Iter to stop. It must be
// inspected after Next returns false.
func (it *Iter) Err() error {
	return it.err
}

// Next advances the Iter to the next item in the list, which will then be
// available through the Current method. It returns false when the iterator
// stops at the end of the list.
func (it *Iter) Next() bool {
	if len(it.values) == 0 && it.nextStartKey != nil {
		// get more pages
		it.getNextPage()
	}

	if len(it.values) == 0 {
		// we are finished here
		return false
	}

	it.cur = it.values[0]
	it.values = it.values[1:]

	return true
}

func (it *Iter) getNextPage() {
	input := &dynamodb.ScanInput{
		ExclusiveStartKey: it.nextStartKey,
		TableName:         it.b.TableName,
	}

	out, err := it.b.Service.Scan(input)
	if err != nil {
		it.err = err
		return
	}

	fmt.Printf("[DEBUG] last %+v (%T)\n", out.LastEvaluatedKey, out.LastEvaluatedKey)
	it.nextStartKey = out.LastEvaluatedKey
	it.values = out.Items
}
