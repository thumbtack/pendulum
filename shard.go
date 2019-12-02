package main

import (
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	logging "github.com/sirupsen/logrus"
)

func (listener *streamListener) waitForParentShard(shardId *string, parentShardId *string) {
	var curWaitTime time.Duration = 0
	for {
		listener.shardLock.Lock()
		_, isFinishedProcessing := listener.completedShards[*parentShardId]
		listener.shardLock.Unlock()

		if isFinishedProcessing {
			logger.WithFields(logging.Fields{
				"Table": listener.streamOwner,
				"Parent ShardId": *parentShardId,
				"ShardId": *shardId,
			}).Debug("Finished processing parent shard")
			return
		}

		time.Sleep(time.Duration(shardWaitForParentInterval))
		curWaitTime += shardWaitForParentInterval

		if curWaitTime >= maxParentWaitInterval {
			logger.WithFields(logging.Fields{
				"Table": listener.streamOwner,
				"Parent ShardId": *parentShardId,
				"ShardId": *shardId,
			}).Error("Taking too long to process parent")
		}
	}
}

func (listener *streamListener) processShard(shard *dynamodbstreams.Shard, isBidirectional bool) {
	var records *dynamodbstreams.GetRecordsOutput

	parentShardId := shard.ParentShardId
	shardId := shard.ShardId

	// process parent shard first
	if parentShardId != nil {
		listener.waitForParentShard(shardId, parentShardId)
	}

	shardIterator, err := listener.getShardIterator(*shardId)
	if err != nil {
		logger.WithFields(logging.Fields{
			"Table": listener.streamOwner,
			"ShardId": *shardId,
		}).Error("Failed to get shard iterator")
	}

	// when nil, the shard has been closed and
	// the iterator will not return any more data
	for shardIterator != nil {
		records, err = listener.getRecords(shardId, *shardIterator)
		if err != nil {
			logger.WithFields(logging.Fields{
				"Table": listener.streamOwner,
				"ShardId": *shardId,
			}).Error("Failed to get records")
			listener.markShardComplete(shardId)
			return
		}
		if len(records.Records) > 0 {
			listener.processSelfRecords(records.Records, isBidirectional)
		}
		shardIterator = records.NextShardIterator
	}
	// shard has been closed
	logger.WithFields(logging.Fields{
		"Table": listener.streamOwner,
		"ShardId": *shardId,
	}).Info("Finished processing shard")
	listener.markShardComplete(shardId)
}

func (listener *streamListener) markShardComplete(shardId *string) {
	listener.shardLock.Lock()
	delete(listener.activeShardProcessors, *shardId)
	listener.completedShards[*shardId] = true
	listener.shardLock.Unlock()
}
