package main

import (
	"errors"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	logging "github.com/sirupsen/logrus"
)

func (listener *streamListener) processSelfRecords(records []*dynamodbstreams.Record, isBidirectional bool) {
	for _, r := range records {
		isDeleteItem := *r.EventName == "REMOVE"
		isFreshItem := r.Dynamodb.NewImage[replicationSource] == nil || r.Dynamodb.NewImage[replicationTimestamp] == nil
		isUpdatedFromClient := r.Dynamodb.NewImage[replicationTimestamp] != nil &&
			r.Dynamodb.OldImage[replicationTimestamp] != nil &&
			*r.Dynamodb.NewImage[replicationTimestamp].N == *r.Dynamodb.OldImage[replicationTimestamp].N

		if isDeleteItem && r.Dynamodb.NewImage == nil {
			// no need to update table; just send it to channel
			newRec := listener.getNewRecord(r)
			listener.recordQueue <- newRec
			continue
		}

		if isFreshItem || isUpdatedFromClient {
			// Update metadata & add to channel
			newRec := listener.getNewRecord(r)

			if isBidirectional {
				err := listener.updateTable(newRec)
				if err != nil {
					logger.WithFields(logging.Fields{
						"Table": listener.streamOwner,
						"Record Old Image": r.Dynamodb.OldImage,
						"Record New Image": r.Dynamodb.NewImage,
						"Error": err,
					}).Error("Failed to update table with Pendulum fields")

					// To continue or not to continue?
					continue
				}
			}

			listener.recordQueue <- newRec
		} else {
			// Discard record if it is an item updated by sync
			// nothing to do
			logger.WithFields(logging.Fields{
				"Table": listener.streamOwner,
				"Record Key": r.Dynamodb.Keys,
				"Record Old Image": r.Dynamodb.OldImage,
				"Record New Image": r.Dynamodb.NewImage,
			}).Debug("Ignoring record")
		}
	}
}

func (listener *streamListener) getNewRecord(record *dynamodbstreams.Record) (*dynamodbstreams.Record) {
	item := record.Dynamodb.NewImage

	// item can be nil if it is a record from "REMOVE" action
	if item == nil {
		item = map[string]*dynamodb.AttributeValue{}
	}

	ts := strconv.FormatInt(record.Dynamodb.ApproximateCreationDateTime.Unix(), 10)
	updateSource := dynamodb.AttributeValue{S: aws.String(listener.streamOwner)}
	updateTimestamp := dynamodb.AttributeValue{N: aws.String(ts)}

	item[replicationSource] = &updateSource
	item[replicationTimestamp] = &updateTimestamp

	cur := record.Dynamodb
	cur.NewImage = item
	newRec := record.SetDynamodb(cur)

	return newRec
}

func (listener *streamListener) updateTable(record *dynamodbstreams.Record) (error){
	var err error = nil
	switch *record.EventName {
	case "MODIFY":
		// same as INSERT
		fallthrough
	case "INSERT":
		err = listener.insertRecord(record.Dynamodb.NewImage)
	case "REMOVE":
		err = listener.removeRecord(record.Dynamodb.Keys)
	default:
		// Should not reach here unless AWS Dynamodb change Event Names or
		// add new one(s)
		err = errors.New("unknown event on record")
	}
	if err != nil {
		logger.WithFields(logging.Fields{
			"Table":  listener.streamOwner,
			"Record": *record.Dynamodb.SequenceNumber,
			"Key":    record.Dynamodb.Keys,
			"Error":  err,
		}).Error("Failed to handle record")
		return err
	}

	logger.WithFields(logging.Fields{
		"Table": listener.streamOwner,
		"Record": *record.Dynamodb.SequenceNumber,
		"Key": record.Dynamodb.Keys,
		"New Record": record.Dynamodb.NewImage,
	}).Debug("Handled record successfully")

	return nil
}

func (listener *streamListener) extractTableFromOwner() string {
	return strings.Split(listener.streamOwner, ".account.")[0]
}

func (listener *streamListener) insertRecord(item map[string]*dynamodb.AttributeValue) (error) {
	input := &dynamodb.PutItemInput{
		Item: item,
		TableName: aws.String(listener.extractTableFromOwner()),
	}
	for i := 1; i <= maxRetries; i++ {
		_, err := listener.dynamo.PutItem(input)
		if err != nil {
			if i == maxRetries {
				return err
			}
			backoff(i, "PutItem")
		} else {
			return nil
		}
	}

	return nil
}

func (listener *streamListener) removeRecord(item map[string]*dynamodb.AttributeValue) (error) {
	var err error = nil
	for i := 1; i <= maxRetries; i++ {
		_, err = listener.dynamo.DeleteItem(&dynamodb.DeleteItemInput{
			TableName: aws.String(listener.extractTableFromOwner()),
			Key:       item,
		})
		if err != nil {
			if i == maxRetries {
				break
			} else {
				backoff(i, "DeleteItem")
			}
		} else {
			break
		}
	}
	return err
}