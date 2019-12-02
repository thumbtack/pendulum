package main

import (
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/sirupsen/logrus"
)

const tableStatusWaitInterval = 1 * time.Second

func describeTable(tableName string, ddbClient *dynamodb.DynamoDB) (*dynamodb.DescribeTableOutput, error) {
	var result *dynamodb.DescribeTableOutput
	var err error
	for i := 1; i <= maxRetries; i++ {
		result, err = ddbClient.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			backoff(i, "Describe Table")
		} else {
			return result, err
		}
	}
	return nil, err
}
func getStreamArn(tableName string, ddbClient *dynamodb.DynamoDB) (string, error) {
	var result *dynamodb.DescribeTableOutput
	var err error

	result, err = describeTable(tableName, ddbClient)
	if result != nil {
		if result.Table.StreamSpecification == nil || *result.Table.StreamSpecification.StreamEnabled == false {
			return "", errors.New("stream not enabled")
		}
		return *result.Table.LatestStreamArn, nil
	}

	return "", err
}

func (listener *streamListener) describeStream(lastShard string) (*dynamodbstreams.DescribeStreamOutput, error) {
	var err error = nil
	var streamDescription *dynamodbstreams.DescribeStreamOutput

	input := &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(listener.streamArn),
		Limit:     aws.Int64(100),
	}

	if lastShard != "" {
		input.ExclusiveStartShardId = aws.String(lastShard)
	}

	for i := 1; i <= maxRetries; i++ {
		streamDescription, err = listener.stream.DescribeStream(input)
		if err != nil {
			backoff(i, "DescribeStream")
		} else {
			return streamDescription, nil
		}
	}
	return nil, err
}

func (listener *streamListener) getShardIterator(shardId string) (*string, error) {
	shardIteratorInput := &dynamodbstreams.GetShardIteratorInput{
		StreamArn: aws.String(listener.streamArn),
		ShardIteratorType:aws.String("TRIM_HORIZON"),
		ShardId: aws.String(shardId),
	}

	for i := 1; i <= maxRetries; i++ {
		iterator, err := listener.stream.GetShardIterator(shardIteratorInput)
		if err != nil {
			backoff(i, "GetShardIterator")
		} else {
			return iterator.ShardIterator, nil
		}
	}

	return nil, nil
}

func (listener *streamListener) getRecords(shardId *string, shardIterator string) (
	*dynamodbstreams.GetRecordsOutput, error) {
		var err error
		var records *dynamodbstreams.GetRecordsOutput
		for i := 1; i <= maxRetries; i++ {
			records, err = listener.stream.GetRecords(
				&dynamodbstreams.GetRecordsInput{
					ShardIterator: aws.String(shardIterator),
				})
			if err != nil {
				backoff(i, "GetRecords")
			} else {
				return records, nil
			}
		}
		return nil, err
}

func getCapacity(table string, dynamodbClient *dynamodb.DynamoDB) (dynamodb.ProvisionedThroughput, error) {
	var result = dynamodb.ProvisionedThroughput{}
	var err error
	var output *dynamodb.DescribeTableOutput

	output, err = describeTable(table, dynamodbClient)

	if err != nil {
		logger.WithFields(logrus.Fields{
			"Table": table,
			"Error": err,
		}).Error("Failed to get table capacity")
		return result, err
	}

	result = dynamodb.ProvisionedThroughput{
		ReadCapacityUnits: output.Table.ProvisionedThroughput.ReadCapacityUnits,
		WriteCapacityUnits: output.Table.ProvisionedThroughput.WriteCapacityUnits,
	}

	return result, err
}

func updateCapacity(table string, dynamodbClient *dynamodb.DynamoDB, newThroughput *dynamodb.ProvisionedThroughput) (error) {
	var err error

	input := &dynamodb.UpdateTableInput {
		TableName: aws.String(table),
		ProvisionedThroughput: newThroughput,
	}

	for i := 1; i <= maxRetries; i++ {
		_, err = dynamodbClient.UpdateTable(input)
		if err != nil {
			backoff(i, "Update Table")
		} else {
			return nil
		}
	}

	logger.WithFields(logrus.Fields{
		"Table": table,
		"Error": err,
	}).Error("Failed to update capacity")

	return err
}

func getStreamStatus(table string, dynamodbClient *dynamodb.DynamoDB) (bool, error) {
	var err error
	var result *dynamodb.DescribeTableOutput

	result, err = describeTable(table, dynamodbClient)
	if err != nil {
		return false, err
	}
	if result.Table.StreamSpecification != nil {
		return *result.Table.StreamSpecification.StreamEnabled, nil
	}

	return false, err
}

func enableStream(table string, dynamodbClient *dynamodb.DynamoDB) error {
	var err error
	input := &dynamodb.UpdateTableInput{
		TableName: aws.String(table),
		StreamSpecification: &dynamodb.StreamSpecification{
			StreamEnabled: aws.Bool(true),
			StreamViewType: aws.String(dynamodb.StreamViewTypeNewAndOldImages)},
	}

	for i := 1; i <= maxRetries; i++ {
		_, err = dynamodbClient.UpdateTable(input)
		if err != nil {
			backoff(i, "Update Table")
		} else {
			return nil
		}
	}

	return err
}

func disableStream(table string, dynamodbClient *dynamodb.DynamoDB) error {
	var err error

	input := &dynamodb.UpdateTableInput{
		TableName:           aws.String(table),
		StreamSpecification: &dynamodb.StreamSpecification{StreamEnabled: aws.Bool(false)},
	}

	for i := 1; i <= maxRetries; i++ {
		_, err = dynamodbClient.UpdateTable(input)
		if err != nil {
			backoff(i, "Update Table")
		} else {
			status := ""
			for status != "ACTIVE" {
				time.Sleep(tableStatusWaitInterval)
				result, err := describeTable(table, dynamodbClient)
				if err != nil {
					return err
				}
				status = *result.Table.TableStatus
			}
			return nil
		}
	}
	return err
}

func batchWrite(items map[string][]*dynamodb.WriteRequest, dynamodbClient *dynamodb.DynamoDB) (
	*dynamodb.BatchWriteItemOutput, error) {
		var err error
		var output *dynamodb.BatchWriteItemOutput
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: items,
			ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		}

		output, err = dynamodbClient.BatchWriteItem(input)
		return output, err
}

func scanTable(table string, id int, totalSegments int, lastEvaluatedKey map[string]*dynamodb.AttributeValue,
	dynamodbClient *dynamodb.DynamoDB) (*dynamodb.ScanOutput, error) {
		var output *dynamodb.ScanOutput
		var err error

		input := &dynamodb.ScanInput{
			TableName: aws.String(table),
			ConsistentRead: aws.Bool(true),
			Segment: aws.Int64(int64(id)),
			TotalSegments: aws.Int64(int64(totalSegments)),
		}

		if len(lastEvaluatedKey) > 0 {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		for i := 1; i <= maxRetries; i++ {
			output, err = dynamodbClient.Scan(input)
			if err != nil {
				backoff(i, "Scan")
			} else {
				return output, nil
			}
		}

		return nil, err
}