package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/sirupsen/logrus"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"
)

var dynamodbClient *dynamodb.DynamoDB
var testRunTime = 1800 * time.Second

type testSuite struct {
	srcTable string
	dstTable string
}

const testTableKey   = "BDR_Key"
const testAttribute  = "test_pendulum"

func putItem(table string, item map[string]*dynamodb.AttributeValue) (error) {
	var err error
	input := &dynamodb.PutItemInput {
		TableName: aws.String(table),
		Item:      item,
	}
	for i := 1; i <= maxRetries; i++ {
		_, err = dynamodbClient.PutItem(input)
		if err == nil {
			return nil
		}
	}

	return err
}

func getItem(table string, itemKey map[string]*dynamodb.AttributeValue) (map[string]*dynamodb.AttributeValue, error) {
	var err error
	var result *dynamodb.GetItemOutput

	input := &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: itemKey,
		ConsistentRead: aws.Bool(true),
	}

	time.Sleep(2000*time.Millisecond)

	for i := 1; i <= maxRetries; i++ {
		result, err = dynamodbClient.GetItem(input)
		if err == nil {
			return result.Item, nil
		}
	}


	logger.WithFields(logrus.Fields{"Item Key": itemKey, "Table": table, "Error": err}).Error("Failed to get item")

	return nil, err
}

func updateItem(table string, itemKey map[string]*dynamodb.AttributeValue, newVal string) (error) {
	expr := fmt.Sprintf("set %s = :n", testAttribute)
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(table),
		Key: itemKey,
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":n": {
				S: aws.String(newVal),
			}},
		UpdateExpression: aws.String(expr),
	}
	_, err := dynamodbClient.UpdateItem(input)
	return err
}

func deleteItem(table string, itemKey map[string]*dynamodb.AttributeValue) (error) {
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(table),
		Key:       itemKey,
	}

	_, err := dynamodbClient.DeleteItem(input)
	return err
}

func (t *testSuite) insertMultiple(tableA string, ch chan bool,
	itemsChan chan map[string]*dynamodb.AttributeValue, putWG *sync.WaitGroup) {
	defer putWG.Done()
	for {
		select {
		case <-ch:
			return
		default:
			item := makeItem()
			err := putItem(tableA, item)
			logger.WithFields(logrus.Fields{"Item Key": item[testTableKey], "Item Val": item[testAttribute]}).Debug("Inserting item")
			if err != nil {
				// skip inserting this to the channel
				continue
			}
			itemsChan <- item
		}
	}
}

func (t *testSuite) queryTable(table string, ch chan bool,
	itemsChan chan map[string]*dynamodb.AttributeValue,
	resultsChan chan bool, getWG *sync.WaitGroup) {
	defer getWG.Done()
	numFail := 0
	for {
		select {
		case itemA, more := <-itemsChan:
			if !more {
				return
			}
			itemB, err := getItem(table, map[string]*dynamodb.AttributeValue{testTableKey: itemA[testTableKey]})
			if err != nil {
				logger.WithFields(logrus.Fields{
					"Table": table,
					"Error": err,
				}).Error("Failed to get item")
				resultsChan <-false
				numFail = numFail+1
			} else {
				isSame := compareItems(itemA, itemB)
				if !isSame {
					logger.WithFields(logrus.Fields{"ItemA": itemA, "ItemB": itemB}).Error("Items are different")
				}
				if !isSame && numFail == 0 {
					resultsChan <-false
					numFail = numFail + 1
				}
			}
		}
	}
}

func compareItems(itemA map[string]*dynamodb.AttributeValue, itemB map[string]*dynamodb.AttributeValue) (bool) {
	logger.WithFields(logrus.Fields{"Item A": itemA, "Item B": itemB}).Debug("Comparing items")
	// check if keys & values in A match those in B
	for keyA := range itemA {
		if keyA == "DDB_REPLICATION_TIMESTAMP" || keyA == "DDB_REPLICATION_SOURCE" {
			continue
		}
		valB, ok := itemB[keyA]
		if !ok {
			return false
		}
		if *valB.S != *itemA[keyA].S {
			return false
		}
	}

	// check if keys & values in B match those in A
	for keyB := range itemB {
		if keyB == "DDB_REPLICATION_TIMESTAMP" || keyB == "DDB_REPLICATION_SOURCE" {
			continue
		}
		valA, ok := itemA[keyB]
		if !ok {
			return false
		}
		if *valA.S != *itemB[keyB].S {
			return false
		}
	}

	return true
}

func generateRandomString() (string) {
	minSize := 4
	maxSize := 10
	n := rand.Intn(maxSize - minSize) + minSize
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	random := make([]byte, n)
	for i := range random {
		random[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(random)
}

func removeIndex(arr []map[string]*dynamodb.AttributeValue, index int) []map[string]*dynamodb.AttributeValue {
	return append(arr[:index], arr[index+1:]...)
}


func makeItem() map[string]*dynamodb.AttributeValue {
	item := make(map[string]*dynamodb.AttributeValue)
	item[testTableKey] = &dynamodb.AttributeValue{S: aws.String(generateRandomString())}
	item[testAttribute] = &dynamodb.AttributeValue{S: aws.String(generateRandomString())}
	return item
}

func makeItemVal(item map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue {
	item[testTableKey] = &dynamodb.AttributeValue{S: aws.String(generateRandomString())}
	return item
}

func (t *testSuite) killRoutine(d time.Duration, quit chan bool) {
	time.Sleep(d)
	quit <-true
}

func newTestSuite(src string, dst string) (*testSuite) {
	tr := &http.Transport{
		MaxIdleConns: 2048,
		MaxIdleConnsPerHost: 1024,
	}
	httpClient := &http.Client{
		Timeout: 8*time.Second,
		Transport: tr,
	}
	region := "us-east-1"
	endpoint := ""
	retries := 3
	sess := session.Must(session.NewSession(aws.NewConfig().
		WithRegion(region).
		WithEndpoint(endpoint).
		WithMaxRetries(retries).
		WithHTTPClient(httpClient)))

	dynamodbClient = dynamodb.New(sess, &aws.Config{})

	return &testSuite{
		srcTable: src,
		dstTable: dst,
	}
}

func (t *testSuite) testInsert() (map[string]*dynamodb.AttributeValue, bool) {
	// Insert in A and check if item exists in B
	itemA := makeItem()
	err := putItem(t.srcTable, itemA)
	if err != nil {
		logger.WithFields(logrus.Fields{"Table": t.srcTable, "Error": err}).Error("Error in inserting item")
		return nil, false
	}

	itemB := map[string]*dynamodb.AttributeValue{testTableKey: itemA[testTableKey]}
	itemB, err = getItem(t.dstTable, itemB)

	isSame := compareItems(itemA, itemB)
	if !isSame {
		logger.WithFields(logrus.Fields{"Item A": itemA, "Item B": itemB, "Table A": t.srcTable, "Table B": t.dstTable}).
			Error("Failed to replicate item inserted")
		return itemA, false
	}

	return itemA, true
}

func (t *testSuite) testModify(itemKey map[string]*dynamodb.AttributeValue) (bool) {
	var err error
	var itemA, itemB map[string]*dynamodb.AttributeValue
	// Update in A and check if the update propagated to B
	err = updateItem(t.srcTable, itemKey, generateRandomString())
	if err != nil {
		logger.WithFields(logrus.Fields{"Item Key": itemKey, "Table": t.srcTable, "Error": err}).Error("Failed to update item")
		return false
	}
	itemA, err = getItem(t.srcTable, itemKey)
	itemB, err = getItem(t.dstTable, itemKey)
	if err != nil {
		return false
	}

	isSame := compareItems(itemA, itemB)
	if !isSame {
		return false
	}

	return true
}

func (t *testSuite) testDelete(itemKey map[string]*dynamodb.AttributeValue) (bool) {
	err := deleteItem(t.srcTable, itemKey)
	if err != nil {
		logger.WithFields(logrus.Fields{"Table": t.srcTable, "Key": itemKey, "Error": err}).Error("Failed to delete item")
		return false
	}

	// check if item is deleted from B
	item, err := getItem(t.dstTable, itemKey)
	if err != nil {
		return false
	}

	// item returned should be empty
	if item != nil {
		return false
	}

	return true
}

func (t *testSuite) testParallelWrites() bool {
	numWorkers := 10
	itemKeys := make([]map[string]*dynamodb.AttributeValue, numWorkers)
	for i := 0; i < numWorkers; i++ {
		itemA := makeItem()
		itemB := map[string]*dynamodb.AttributeValue{testTableKey: itemA[testTableKey]}
		itemB = makeItemVal(itemB)
        go putItem(t.srcTable, itemA)
		go putItem(t.dstTable, itemB)
		itemKeys[i] = map[string]*dynamodb.AttributeValue{testTableKey: itemA[testTableKey]}
	}

	for _, each := range itemKeys {
		itemA, err := getItem(t.srcTable, each)
		if err != nil {
			return false
		}
		itemB, err := getItem(t.dstTable, each)
		if err != nil {
			return false
		}

		isSame := compareItems(itemA, itemB)
		if !isSame {
			return false
		}
	}

	return true
}

func (t *testSuite) testBulkWrites() bool{
	numWorkers := 10
	var getWG, putWG sync.WaitGroup
	itemsChan := make([]chan map[string]*dynamodb.AttributeValue, numWorkers)
	resultsChan := make(chan bool, numWorkers)
	quitChan := make([]chan  bool, numWorkers)

	putWG.Add(numWorkers)
	getWG.Add(numWorkers)

	// Write to Table A for 30 minutes
	for i := 0; i < numWorkers; i++ {
		itemsChan[i] = make(chan map[string]*dynamodb.AttributeValue)
		quitChan[i] = make(chan bool)
		go t.insertMultiple(t.srcTable, quitChan[i], itemsChan[i], &putWG)
		go t.queryTable(t.dstTable, quitChan[i], itemsChan[i], resultsChan, &getWG)
		go t.killRoutine(testRunTime, quitChan[i])
	}

	putWG.Wait()
	for i := 0; i < numWorkers; i++ {
		close(itemsChan[i])
	}
	getWG.Wait()
	close(resultsChan)

	for ok := range resultsChan {
		if !ok {
			return false
		}
	}

	return true
}

func (t *testSuite) testRandomAction() bool {
	numActions := 3
	items := make([]map[string]*dynamodb.AttributeValue, 0)
	quitChan := make(chan bool)
	var action int

	go t.killRoutine(testRunTime, quitChan)
	for {
		select {
		case <-quitChan:
			return true
		default:
			if len(items) == 0 {
				action = 0
			} else {
				action = rand.Intn(10000) % numActions
			}
			switch action {
			case 0:
				// Insert
				item, ok := t.testInsert()
				if !ok {
					return false
				}
				items = append(items, item)
			case 1:
				// Modify
				pick := rand.Intn(len(items))
				ok := t.testModify(map[string]*dynamodb.AttributeValue{testTableKey: items[pick][testTableKey]})
				if !ok {
					return false
				}
			case 2:
				// Delete
				pick := rand.Intn(len(items))
				ok := t.testDelete(map[string]*dynamodb.AttributeValue{testTableKey: items[pick][testTableKey]})
				if !ok {
					return false
				}
				items = removeIndex(items, pick)
			}
		}
	}
}

func TestAll(t *testing.T) {
	var ok bool
	table1 := "pendulum.testA"
	table2 := "pendulum.testB"
	test := newTestSuite(table1, table2)
	rand.Seed(time.Now().UnixNano())

	numItems := 10
	items := make([]map[string]*dynamodb.AttributeValue, numItems)
	i := 0

	// Insert N items
	for j := 0; j < numItems; j++ {
		items[i] = make(map[string]*dynamodb.AttributeValue)
		items[i], ok = test.testInsert()
		logger.Info(items[i])
		if !ok {
			t.FailNow()
		}
		i = i+1
	}

   logger.Info("Insert N Items test passed")

	// Modify the N items inserted
	for j := 0; j < numItems; j++  {
		ok = test.testModify(map[string]*dynamodb.AttributeValue{testTableKey: items[j][testTableKey]})
		if !ok {
			t.FailNow()
		}
	}

   logger.Info("Modify N Items test passed")

	// Delete P of N items
	numDeleteItems := 5
	for j := 0; j < numDeleteItems; j++ {
		n := rand.Intn(len(items))
		key := map[string]*dynamodb.AttributeValue{testTableKey: items[n][testTableKey]}
		ok = test.testDelete(key)
		if !ok {
			t.FailNow()
		}
		items = removeIndex(items, n)
	}

   logger.Info("Delete Items test passed")

	// Test Parallel Writes
	ok = test.testParallelWrites()
	if !ok {
		t.FailNow()
	}

   logger.Info("Parallel Items test passed")

	// Test Bulk Writes
	ok = test.testBulkWrites()
	if !ok {
		logger.Error("Bulk writes failed")
		t.FailNow()
	}

   logger.Info("Bulk Writes test passed")

	// Interleave tests
	ok = test.testRandomAction()
	if !ok {
		logger.Error("Interleave test failed")
		t.FailNow()
	}

   logger.Info("Interleave test passed")

	logger.Info("Looks ok, good to go!")
	os.Exit(0)
}
