package main

import (
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/sirupsen/logrus"
)

const (
	shardEnumerateInterval     = 30 * time.Second
	shardWaitForParentInterval = 20 * time.Second
	maxParentWaitInterval      = 15 * 60 * time.Second
	waitForStream              = 20 * time.Second
	maxWaitForStream           = 5 * 60 * time.Second
)

func checkAndDisableStream(table string, client *dynamodb.DynamoDB) error {
	isStreamEnabled, err := getStreamStatus(table, client)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"Error": err,
			"Table": table,
		}).Error("Failed to get stream status")
		return err
	}

	if isStreamEnabled {
		logger.WithFields(logrus.Fields{
			"Table": table,
		}).Debug("Disabling stream")

		err = disableStream(table, client)

		if err != nil {
			logger.WithFields(logrus.Fields{
				"Error": err,
				"Table": table,
			}).Error("Failed to disable stream on table. Please disable stream before proceeding")
			return err
		}
	}
	
	return nil
}

func refreshStream(table string, client *dynamodb.DynamoDB) error {
	err := checkAndDisableStream(table, client)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"Error": err,
			"Table": table,
		}).Error("Failed to refresh table stream")
		return err
	}

	logger.WithFields(logrus.Fields{
		"Table": table,
	}).Info("Re-enabling stream")

	err = enableStream(table, client)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"Error": err,
			"Table": table,
		}).Error("Failed to refresh table stream")
		return err
	}
	return nil
}

func waitAndGetStreamARN(table string, client *dynamodb.DynamoDB) (string, error) {
	var streamArn string
	var err error
	var waitTime time.Duration = 0
	for {
		streamArn, err = getStreamArn(table, client)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"Error": err,
				"Table": table,
			}).Error("Failed to get stream ARN. Waiting for stream to be enabled")
			time.Sleep(waitForStream)
			waitTime += waitForStream
			if waitTime >= maxWaitForStream {
				return "", err
			}
		} else {
			logger.WithFields(logrus.Fields{
				"Table": table,
				"Stream ARN": streamArn,
			}).Info("Updated latest stream ARN")
			return streamArn, nil
		}
	}
}

func (state *syncState) replicate() {
	// refresh stream on source by disabling & enabling stream before we start copying over tables
	err := refreshStream(state.tableConfig.SrcTable, state.srcStreamListener.dynamo)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"Error": err,
			"Table": state.tableConfig.SrcTable,
		}).Error("Failed to refresh stream")
		return
	}
	
	// reset stream ARN in source table
	state.srcStreamListener.streamArn, err = waitAndGetStreamARN(state.tableConfig.SrcTable,
		state.srcStreamListener.dynamo)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"Error":err,
			"Table":state.tableConfig.SrcTable}).Error("Failed to get stream ARN")
		return
	}

	// Phase 1: First copy the tables
	err = state.copyTable()

	if err != nil {
		logger.WithFields(logrus.Fields{
			"Error":        err,
			"Source Table": state.tableConfig.SrcTable,
			"Dst Table":    state.tableConfig.DstTable}).Error("Failed to copy tables")
		return
	}

	// refresh stream on destination by disabling & enabling stream
	err = refreshStream(state.tableConfig.DstTable, state.dstStreamListener.dynamo)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"Error": err,
			"Table":state.tableConfig.DstTable,
		}).Error("Failed to refresh stream")
		return
	}
	// get the stream ARN
	state.dstStreamListener.streamArn, err = waitAndGetStreamARN(state.tableConfig.DstTable,
		state.dstStreamListener.dynamo)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"Error": err,
			"Table": state.tableConfig.DstTable,
		}).Error("Failed to get stream ARN")
		return
	}

	// Phase 2: Uni-sync until all records that were written to src during the copy are copied over


	// Phase 3: Bi-directional sync
	// Start processing srcStream
	quitSrc := make(chan bool)
	quitDst := make(chan bool)
	state.srcStreamListener.listen(state.dstStreamListener.recordQueue, quitSrc, quitDst)
	state.dstStreamListener.listen(state.srcStreamListener.recordQueue, quitDst, quitSrc)
}
