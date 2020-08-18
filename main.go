/*
 Copyright 2018 Thumbtack, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

/* Note: Although the tool allows multiple destinations to sync from a single source srcStream,
 * AWS DynamoDb Streams documentation specifies that, having more than 2 readers per shard
 * can result in throttling
 * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
 */

package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	logging "github.com/sirupsen/logrus"
)

const (
	paramMaxRetries           = "MAX_RETRIES"
	paramVerbose              = "VERBOSE"
	paramPort                 = "PORT"
	paramConfigDir            = "CONFIG_DIR"
	defaultConfigMaxRetries   = 3
	replicationSource         = "DDB_REPLICATION_SOURCE"
	replicationTimestamp      = "DDB_REPLICATION_TIMESTAMP"
	maxExponentialBackoffTime = 2048
)

var logger = logging.New()
var maxRetries = defaultConfigMaxRetries

type loadConfig struct {
	ReadQPS      int64 `json:"read_qps"`
	WriteQPS     int64 `json:"write_qps"`
	ReadWorkers  int   `json:"read_workers"`
	WriteWorkers int   `json:"write_workers"`
}

type config struct {
	SrcTable    string     `json:"src_table"`
	DstTable    string     `json:"dst_table"`
	SrcRegion   string     `json:"src_region"`
	DstRegion   string     `json:"dst_region"`
	SrcEndpoint string     `json:"src_endpoint"`
	DstEndpoint string     `json:"dst_endpoint"`
	SrcEnv      string     `json:"src_env"`
	DstEnv      string     `json:"dst_env"`
	CopyLoad    loadConfig `json:"copy_load"`
}

// Config file is read and dumped into this struct
type syncState struct {
	tableConfig       config
	srcStreamListener streamListener
	dstStreamListener streamListener
}

type streamListener struct {
	streamOwner           string
	dynamo                *dynamodb.DynamoDB
	stream                *dynamodbstreams.DynamoDBStreams
	streamArn             string
	recordQueue           chan *dynamodbstreams.Record
	recordCache           map[tableKey]int64
	activeShardProcessors map[string]bool
	shardLock             sync.RWMutex
	completedShards       map[string]bool
}

type tableKey struct {
	itemKey string
}

func getRoleArn(env string) string {
	roleType := strings.ToUpper(env) + "_ROLE"
	logger.WithFields(logging.Fields{"RoleARN": os.Getenv(roleType)}).Debug()
	return os.Getenv(roleType)
}

func newStreamListener() *streamListener {
	return &streamListener{
		dynamo:                nil,
		stream:                nil,
		streamOwner:           "",
		streamArn:             "",
		recordCache:           map[tableKey]int64{},
		activeShardProcessors: map[string]bool{},
		shardLock:             sync.RWMutex{},
		completedShards:       map[string]bool{},
		recordQueue:           make(chan *dynamodbstreams.Record),
	}
}

func newDynamodb(region string,
	endpoint string,
	environment string) (*dynamodb.DynamoDB, *dynamodbstreams.DynamoDBStreams) {
	tr := &http.Transport{
		MaxIdleConns:    2048,
		MaxConnsPerHost: 1024,
	}

	httpClient := &http.Client{
		Timeout:   8 * time.Second,
		Transport: tr,
	}

	sess := session.Must(
		session.NewSession(
			aws.NewConfig().
				WithRegion(region).
				WithEndpoint(endpoint).
				WithMaxRetries(maxRetries).
				WithHTTPClient(httpClient),
		))

	// No need to assume role if Pendulum is not run remotely
	if environment == "local" {
		return dynamodb.New(sess, &aws.Config{}), dynamodbstreams.New(sess, &aws.Config{})
	}

	roleArn := getRoleArn(environment)
	if roleArn == "" {
		logger.WithFields(logging.Fields{}).Error("Failed to get role ARN. Check config")
		return nil, nil
	}
	creds := stscreds.NewCredentials(sess, roleArn)
	logger.WithFields(logging.Fields{"Creds": creds}).Debug()

	dynamo := dynamodb.New(sess, &aws.Config{Credentials: creds})
	stream := dynamodbstreams.New(sess, &aws.Config{Credentials: creds})

	return dynamo, stream
}

// syncState Constructor
func newSyncState(tableConfig config) *syncState {
	var err error
	key := getPrimaryKey(tableConfig)
	srcStreamListener := *newStreamListener()
	srcStreamListener.streamOwner = key.sourceTable
	srcStreamListener.dynamo, srcStreamListener.stream = newDynamodb(
		tableConfig.SrcRegion, tableConfig.SrcEndpoint,
		tableConfig.SrcEnv)
	if srcStreamListener.dynamo == nil || srcStreamListener.stream == nil {
		logger.WithFields(logging.Fields{"Table": tableConfig.SrcTable}).Error("Failed to get dynamo client")
		return nil
	}
	srcStreamListener.streamArn, err = getStreamArn(tableConfig.SrcTable, srcStreamListener.dynamo)
	if err != nil {
		logger.WithFields(logging.Fields{"Table": tableConfig.SrcTable}).Error("Stream not enabled")
		return nil
	}

	dstStreamListener := *newStreamListener()
	dstStreamListener.streamOwner = key.dstTable
	dstStreamListener.dynamo, dstStreamListener.stream = newDynamodb(
		tableConfig.DstRegion, tableConfig.DstEndpoint,
		tableConfig.DstEnv)
	_, err = getStreamArn(tableConfig.DstTable, dstStreamListener.dynamo)
	if err != nil {
		logger.WithFields(logging.Fields{"Table": tableConfig.DstTable}).Error("Stream not enabled")
		return nil
	}

	// dst stream ARN will be set later, after copying the tables
	return &syncState{
		tableConfig:       tableConfig,
		srcStreamListener: srcStreamListener,
		dstStreamListener: dstStreamListener,
	}

}

type appConfig struct {
	sync    []config
	verbose bool
}

// The primary key of the Checkpoint ddb table, of the srcStream etc
// We need the key to be source + dest, since we can have a single
// source being synced with multiple destinations
type primaryKey struct {
	sourceTable string
	dstTable    string
}

// app constructor
func newApp() *appConfig {
	logger.SetLevel(logging.InfoLevel)
	var err error
	var configFile string
	if os.Getenv(paramVerbose) != "" {
		verbose, err := strconv.Atoi(os.Getenv(paramVerbose))
		if err != nil {
			logger.WithFields(logging.Fields{
				"error": err,
			}).Fatal("Failed to parse " + paramVerbose)
		}
		if verbose != 0 {
			logger.SetLevel(logging.DebugLevel)
		}
	}
	if os.Getenv(paramMaxRetries) != "" {
		maxRetries, err = strconv.Atoi(os.Getenv(paramMaxRetries))
		if err != nil {
			logger.WithFields(logging.Fields{
				"error": err,
			}).Fatal("Failed to parse " + paramMaxRetries)
		}
	}

	configFile = os.Getenv(paramConfigDir) + "/config.json"
	tableConfig, err := readConfigFile(configFile, *logger)
	if err != nil {
		os.Exit(1)
	}

	tableConfig, err = setDefaults(tableConfig)
	if err != nil {
		logger.WithFields(logging.Fields{"Error": err}).Error("Error in config file values")
		os.Exit(1)
	}

	return &appConfig{
		sync:    tableConfig,
		verbose: true,
	}
}

// Helper function to read the config file
func readConfigFile(configFile string, logger logging.Logger) ([]config, error) {
	var listStreamConfig []config
	logger.WithFields(logging.Fields{
		"path": configFile,
	}).Debug("Reading config file")
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		logger.Error(err)
		return listStreamConfig, err
	}
	err = json.Unmarshal(data, &listStreamConfig)
	if err != nil {
		logger.Error(err)
		return listStreamConfig, errors.New("failed to unmarshal config")
	}

	return listStreamConfig, nil
}

func verifyEnvValues(env string) bool {
	allowedEnvValues := [8]string{"development", "staging", "production",
		"development_new", "staging_new", "production_new", "shared", "local"}
	for i := 0; i < len(allowedEnvValues); i++ {
		if strings.ToLower(env) == allowedEnvValues[i] {
			return true
		}
	}
	return false
}

func setDefaults(tableConfig []config) ([]config, error) {
	var err error = nil
	for i := 0; i < len(tableConfig); i++ {
		if tableConfig[i].SrcTable == "" ||
			tableConfig[i].DstTable == "" ||
			tableConfig[i].SrcRegion == "" ||
			tableConfig[i].DstRegion == "" ||
			!verifyEnvValues(tableConfig[i].SrcEnv) ||
			!verifyEnvValues(tableConfig[i].DstEnv) {
			err = errors.New("invalid JSON: source and destination table, " +
				"region, and environment are mandatory")
			continue
		}

		tableConfig[i].SrcEnv = strings.ToLower(tableConfig[i].SrcEnv)
		tableConfig[i].DstEnv = strings.ToLower(tableConfig[i].DstEnv)

		if tableConfig[i].CopyLoad.ReadQPS == 0 {
			tableConfig[i].CopyLoad.ReadQPS = 500
		}

		if tableConfig[i].CopyLoad.WriteQPS == 0 {
			tableConfig[i].CopyLoad.WriteQPS = 500
		}

		if tableConfig[i].CopyLoad.ReadWorkers == 0 {
			tableConfig[i].CopyLoad.ReadWorkers = 5
		}

		if tableConfig[i].CopyLoad.WriteWorkers == 0 {
			tableConfig[i].CopyLoad.WriteWorkers = 5
		}
	}

	return tableConfig, err
}

func getPrimaryKey(sync config) primaryKey {
	key := primaryKey{}
	delim := "_"
	if !strings.Contains(sync.SrcEnv, "new") {
		key.sourceTable = sync.SrcTable
	} else {
		key.sourceTable = sync.SrcTable + ".account." + strings.Split(sync.SrcEnv, delim)[0]
	}
	if !strings.Contains(sync.DstEnv, "new") {
		key.dstTable = sync.DstTable
	} else {
		key.dstTable = sync.DstTable + ".account." + strings.Split(sync.DstEnv, delim)[0]
	}
	return key
}

func backoff(i int, s string) {
	wait := math.Pow(2, float64(i))
	if wait > maxExponentialBackoffTime {
		wait = maxExponentialBackoffTime
	}
	logger.WithFields(logging.Fields{
		"Backoff Caller":        s,
		"Backoff Time(seconds)": wait,
	}).Info("Backing off")
	time.Sleep(time.Duration(wait) * time.Second)
}

func main() {
	app := newApp()
	for i := 0; i < len(app.sync); i++ {
		key := getPrimaryKey(app.sync[i])
		syncWorker := newSyncState(app.sync[i])
		if syncWorker == nil {
			logger.WithFields(logging.Fields{
				"Source Table":      key.sourceTable,
				"Destination Table": key.dstTable,
			}).Error("Error in sync configuration")
			os.Exit(1)
		}
		// Call a go routine to replicate for each key
		logger.WithFields(logging.Fields{
			"Source Table":      key.sourceTable,
			"Destination Table": key.dstTable,
		}).Info("Launching replicate")
		// Start listeners to listen to the stream
		go syncWorker.replicate()
	}

	http.HandleFunc("/", syncResponder())
	http.ListenAndServe(":"+os.Getenv(paramPort), nil)
}
