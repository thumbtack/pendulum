package main

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

const (
	maxBatchSize = 25
)

func (state *syncState) copyTable()(error) {
	logger.WithFields(logrus.Fields{
		"Source Table":      state.tableConfig.SrcTable,
		"Destination Table": state.tableConfig.DstTable,
	}).Debug("Starting copying of tables")

	err := state.readAndWrite()

	return err
}

func (state *syncState) readAndWrite() error {
	var writerWG, readerWG sync.WaitGroup

	writeWorkers := state.tableConfig.CopyLoad.WriteWorkers
	readWorkers := state.tableConfig.CopyLoad.ReadWorkers

	scannedItems := make(chan []map[string]*dynamodb.AttributeValue)
	quitCopy := make(chan error)

	writerWG.Add(writeWorkers)

	lim := rate.Limit(state.tableConfig.CopyLoad.WriteQPS)
	rl := rate.NewLimiter(lim, int(state.tableConfig.CopyLoad.WriteQPS))

	for i := 0; i < writeWorkers; i++ {
		go state.writeTable(scannedItems, &writerWG, i, rl)
	}

	readerWG.Add(readWorkers)
	for i := 0; i < readWorkers; i++ {
		go state.readTable(scannedItems, &readerWG, i, quitCopy)
	}

	checkError:
	for {
		select {
		case err := <-quitCopy:
			if err != nil {
				return err
			} else {
				break checkError
			}
		}
	}

	readerWG.Wait()
	close(scannedItems)
	writerWG.Wait()

	logger.WithFields(logrus.Fields{
		"Source Table": state.tableConfig.SrcTable,
		"Destination Table": state.tableConfig.DstTable,
	}).Info("Finished copying dynamodb tables")

	return nil
}

func (state *syncState) writeTable(itemsChan chan []map[string]*dynamodb.AttributeValue,
	writerWG *sync.WaitGroup, id int, rl *rate.Limiter) {
		defer writerWG.Done()
		var writeBatchSize int64
		var reqCapacity float64
		writeRequest := make(map[string][]*dynamodb.WriteRequest, 0)
		dst := state.tableConfig.DstTable
		
		if maxBatchSize > state.tableConfig.CopyLoad.WriteQPS {
			writeBatchSize = state.tableConfig.CopyLoad.WriteQPS
		} else {
			writeBatchSize = maxBatchSize
		}

		for {
			items, more := <-itemsChan
			if !more {
				logger.WithFields(logrus.Fields{"Id": id}).Info("Write worker finished")
				return
			}

			for _, item := range items {
				requestSize := len(writeRequest[dst])
				if int64(requestSize) == writeBatchSize {
					consumedCapacity := state.processBatch(writeRequest, rl, reqCapacity, id, writeBatchSize)
					reqCapacity = 0
					for _, each := range consumedCapacity {
						reqCapacity += *each.CapacityUnits
					}
					writeRequest[dst] = []*dynamodb.WriteRequest{{
						PutRequest: &dynamodb.PutRequest{Item: item},
					}}
				} else {
					writeRequest[dst] = append(writeRequest[dst], &dynamodb.WriteRequest{
						PutRequest: &dynamodb.PutRequest{Item: item},
					})
				}
			}
			// Process items left over (if any)
			// More items may have been left over because len(items) % writeBatchSize != 0
			if len(writeRequest[dst]) > 0 {
				state.processBatch(writeRequest, rl, reqCapacity, id, writeBatchSize)
				writeRequest = make(map[string][]*dynamodb.WriteRequest, 0)
			}
		}
}

func (state *syncState) processBatch(
	batch map[string][]*dynamodb.WriteRequest,
	rl *rate.Limiter, reqCapacity float64,
	id int, writeBatchSize int64) []*dynamodb.ConsumedCapacity {
		logger.WithFields(logrus.Fields{"Id": id}).Info("Trying to acquire write permits")
		r := rl.ReserveN(time.Now(), int(reqCapacity))
		if !r.OK() {
			r = rl.ReserveN(time.Now(), int(writeBatchSize))
		}
		time.Sleep(r.Delay())
		logger.WithFields(logrus.Fields{"Id": id, "Req Capacity": reqCapacity}).Info("Acquired permits")

		consumedCapacity := make([]*dynamodb.ConsumedCapacity, 0)
		i := 0
		for len(batch) > 0 {
			output, _ := batchWrite(batch, state.dstStreamListener.dynamo)
			consumedCapacity = append(consumedCapacity, output.ConsumedCapacity...)

			if output.UnprocessedItems != nil {
				backoff(i, "BatchWrite")
				i++
			}
			batch = output.UnprocessedItems
		}
		return consumedCapacity
}

func (state *syncState) readTable(
	itemsChan chan []map[string]*dynamodb.AttributeValue,
	readerWG *sync.WaitGroup, id int, quitCopy chan error) {
		defer readerWG.Done()

		lastEvaluatedKey := make(map[string]*dynamodb.AttributeValue, 0)

		for {
			result, err := scanTable(state.tableConfig.SrcTable, id, state.tableConfig.CopyLoad.ReadWorkers,
				lastEvaluatedKey, state.srcStreamListener.dynamo)
			if err != nil {
				logger.WithFields(logrus.Fields{
					"Table": state.tableConfig.SrcTable,
					"Error": err,
				}).Error("Failed to scan table")
				quitCopy <- err
			}
			lastEvaluatedKey = result.LastEvaluatedKey
			itemsChan <- result.Items
			if len(lastEvaluatedKey) == 0 {
				logger.WithFields(logrus.Fields{
					"Table": state.tableConfig.SrcTable,
				}).Info("Read worker finished")
				quitCopy <- nil
				return
			}
		}
}