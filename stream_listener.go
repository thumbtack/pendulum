package main

import (
	"time"
	
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	logging "github.com/sirupsen/logrus"
)

const waitForMoreRecords = 10

func (listener *streamListener) listen(altRecords <-chan *dynamodbstreams.Record, quitSelf chan bool, quitAlt chan bool) {
	go listener.processSelfStream(quitSelf, quitAlt)
	go listener.processAltStream(altRecords, quitSelf, quitAlt)
}

func (listener *streamListener) getShards() error {
	var result *dynamodbstreams.DescribeStreamOutput
	var err error
	var isBidirectional = false
	lastEvaluatedShardId := ""

	for {
		result, err = listener.describeStream(lastEvaluatedShardId)
		if err != nil {
			logger.WithFields(logging.Fields{
				"Table": listener.streamOwner,
				"Error": err,
			}).Error("Failed to describe stream")
			return err
		}

		for _, shard := range result.StreamDescription.Shards {
			listener.shardLock.Lock()
			_, isActivelyProcessed := listener.activeShardProcessors[*shard.ShardId]
			_, isCompleted := listener.completedShards[*shard.ShardId]
			listener.shardLock.Unlock()

			if !isActivelyProcessed && !isCompleted {
				logger.WithFields(logging.Fields{
					"Table":   listener.streamOwner,
					"ShardId": *shard.ShardId,
				}).Debug("Starting processor for shard")
				listener.shardLock.Lock()
				listener.activeShardProcessors[*shard.ShardId] = true
				listener.shardLock.Unlock()

				go listener.processShard(shard, isBidirectional)
			} else {
				// Shard is already being processed
				continue
			}
		}

		if result.StreamDescription.LastEvaluatedShardId != nil {
			lastEvaluatedShardId = *result.StreamDescription.LastEvaluatedShardId
		} else {
			// No more shards to be processed for now
			// wait a few seconds before trying again
			// This API cannot be called more than 10/s
			logger.WithFields(logging.Fields{
				"Table": listener.streamOwner,
			}).Debug("No more shards to be processed for now")
			lastEvaluatedShardId = ""
			isBidirectional = true
			time.Sleep(shardEnumerateInterval)
		}
	}
}

func (listener *streamListener) processSelfStream(quitSelf chan bool, quitAlt chan bool) {
	for {
		select {
		case <- quitSelf:
			logger.WithFields(logging.Fields{
				"Table": listener.streamOwner,
			}).Error("Quitting processing stream")
			return
		default:
			err := listener.getShards()
			if err != nil {
				quitAlt <- true
				break
			}
		}
	}
}

func (listener *streamListener) processAltStream(altRecords <-chan *dynamodbstreams.Record, quitSelf chan bool, quitAlt chan bool) {
	for {
		select {
		case record := <-altRecords:
			if *record.Dynamodb.NewImage[replicationSource].S == listener.streamOwner {
				logger.WithFields(logging.Fields{
					"Table": listener.streamOwner,
					"Record Key": record.Dynamodb.Keys,
				}).Debug("Found record in alt stream with myself as source. Ignoring record")

				continue
			}
		    oldT := record.Dynamodb.OldImage[replicationTimestamp]
		    newT := record.Dynamodb.NewImage[replicationTimestamp]

		    if oldT != nil && newT != nil && *oldT.N == *newT.N {
		    	continue
			}

			logger.WithFields(logging.Fields{
				"Table": listener.streamOwner,
				"Record Number": *record.Dynamodb.SequenceNumber,
				"Record Creation Time": record.Dynamodb.ApproximateCreationDateTime,
				"Record Key": record.Dynamodb.Keys,
				"Record New Image": record.Dynamodb.NewImage,
			}).Debug("Received record from alt table")
			listener.updateTable(record)
		case <- quitSelf:
			logger.WithFields(logging.Fields{
				"Table": listener.streamOwner,
			}).Error("Received quit signal")
			return
		default:
			time.Sleep(time.Duration(waitForMoreRecords))
		}
	}
}