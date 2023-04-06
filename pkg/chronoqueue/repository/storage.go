package repository

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
)

type Storage interface {
	CreateQueue(ctx context.Context, queueInfo *chronoqueue.Queue) error
	DeleteQueue(ctx context.Context, queueName string) error
	CreateQueueMessage(ctx context.Context, queueName string, message internal.QueueMessageInfo) error
	GetQueueMessage(ctx context.Context, queueName string, leaseDuration int64) (internal.QueueMessageInfo, error)
	DeleteQueueMessage(ctx context.Context, queueName string, messageID string) error
	AcknowledgeMessage(ctx context.Context, queueName string, messageID string, state internal.State) error
	RenewMessageLease(ctx context.Context, queueName string, leaseDuration int64, messageID string) error
	PeekQueueMessages(ctx context.Context, queueName string, limit int64, priorityRange internal.PriorityRange) ([]internal.QueueMessageInfo, error)
	GetQueueState(ctx context.Context, queueName string) (internal.QueueStateInfo, error)
}

type storage struct {
	redisClient *redis.Client
}

func NewQueueStorage(redisClient *redis.Client) Storage {
	storage := &storage{redisClient: redisClient}
	ctx := context.Background()

	go storage.RunLuaScripts(ctx)
	return storage
}

func (as *storage) CreateQueue(ctx context.Context, queueInfo *chronoqueue.Queue) error {
	txPipeline := as.redisClient.TxPipeline()

	log.Println("RECEIVED QUEUE INFO: ", queueInfo)

	// Check if Queue already exists
	exists, err := txPipeline.Exists(ctx, queueInfo.Name, fmt.Sprintf("%s:meta", queueInfo.Name)).Result()
	if err != nil {
		log.Println("Failed to check queue existance. err: ", err)
		return err
	}
	if exists >= 2 {
		err := errors.New("queue alread exist")
		log.Println("Failed to add message to Queue members. err: ", err)
		return nil
	}

	createResult, err := txPipeline.ZAdd(ctx, queueInfo.Name, redis.Z{}).Result()
	if err != nil {
		log.Println("Failed to create queue. Err: ", err)
		return err
	}
	log.Println("Successfully created Queue. result: ", createResult)
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}

	queueMetadataByte, _ := m.Marshal(queueInfo.Metadata)
	if err != nil {
		log.Println("Failed to marshal queue's meta. Err: ", err)
		return err
	}

	if string(queueInfo.Metadata.Type) == string(internal.Exclusive) {
		if queueInfo.Metadata.ExclusivityKey == "" {
			return errors.New("exclusivity key missing for an EXCLUSIVE queue type")
		} else {
			metaResult, err := txPipeline.HSet(ctx, fmt.Sprintf("%s:meta", queueInfo.Name), "metadata", string(queueMetadataByte)).Result()
			if err != nil {
				log.Println("Failed to create exclusive queue's meta. Err: ", err)
				return err
			}
			log.Println("Successfully created metadata for Queue. result: ", metaResult)
		}
	} else {
		metaResult, err := txPipeline.HSet(ctx, fmt.Sprintf("%s:meta", queueInfo.Name), "metadata", string(queueMetadataByte)).Result()
		if err != nil {
			log.Println("Failed to create non exclusive queue's meta. Err: ", err)
			return err
		}
		log.Println("Successfully created metadata for Queue. result: ", metaResult)
	}
	_, err = txPipeline.Exec(ctx)
	if err != nil {
		log.Println("Failed to create queue. error: ", err)
		return err
	}
	return nil
}

func (as *storage) DeleteQueue(ctx context.Context, queueName string) error {

	checker := NewKeyChecker(as.redisClient, 100)

	start := time.Now()
	checker.Start(ctx)

	iter := as.redisClient.Scan(ctx, 0, fmt.Sprintf("%s*", queueName), 0).Iterator()
	for iter.Next(ctx) {
		checker.Add(iter.Val())
	}
	if err := iter.Err(); err != nil {
		return err
	}

	deleted := checker.Stop()
	log.Println("deleted", deleted, "keys", "in", time.Since(start))

	// txPipeline := as.redisClient.TxPipeline()
	// // send DEL command to Redis to delete the queue
	// err := txPipeline.Del(ctx, queueName).Err()
	// if err != nil {
	// 	return err
	// }
	// // send DEL command to Redis to delete the queue's metadata
	// err = txPipeline.HDel(ctx, fmt.Sprintf("%s:meta", queueName)).Err()
	// if err != nil {
	// 	return err
	// }

	// // send DEL command to Redis to delete the message's metadata for this queue
	// err = txPipeline.HDel(ctx, fmt.Sprintf("%s:*:meta", queueName)).Err()
	// if err != nil {
	// 	return err
	// }
	// _, err = txPipeline.Exec(ctx)
	// if err != nil {
	// 	return err
	// }
	return nil
}

func (as *storage) CreateQueueMessage(ctx context.Context, queueName string, message internal.QueueMessageInfo) error {

	queueKeys := []string{queueName, fmt.Sprintf("%s:meta", queueName)}
	// Check if Queue exists
	// exists, err := as.redisClient.ZCount(ctx, queueName, "-inf", "+inf").Result()
	exists, err := as.redisClient.Exists(ctx, queueKeys...).Result()
	if err != nil {
		log.Println("Failed to add message to Queue members. err: ", err)
		return err
	}
	log.Println("Found exists ==> ", exists)
	if exists < 1 {
		err := errors.New("message's queue does not exist")
		log.Println("Failed to add message to Queue members. err: ", err)
		return err
	}

	if message.InvisibilityDuration == 0 {
		message.InvisibilityDuration = time.Now().Add(0).UnixNano() / int64(time.Millisecond)
		message.State = internal.State(2)
	}
	txPipeline := as.redisClient.TxPipeline()
	// Calculate the message's deadline as a Unix timestamp based on priority
	deadline := time.Now().Add(time.Duration(message.Priority)).UnixNano() / int64(time.Millisecond)
	addMemberResult, err := txPipeline.ZAdd(ctx, queueName, redis.Z{
		Score:  float64(deadline),
		Member: message.MessageID,
	}).Result()
	if err != nil {
		log.Println("Failed to add message to Queue members. err: ", err)
		return err
	}
	log.Println("Successfully added message to Queue members. result: ", addMemberResult)

	// create metadata for non exclusive queue
	metaResult, err := txPipeline.HSet(ctx, fmt.Sprintf("%s:%s:meta", queueName, message.MessageID), message).Result()
	if err != nil {
		log.Println("Failed to create message's metadata. Err: ", err)
		return err
	}

	_, err = txPipeline.Exec(ctx)
	if err != nil {
		log.Println("Failed to execute redis pipe command")
		return err
	}
	log.Println("Successfully created metadata for message. result: ", metaResult)
	return nil
}

func (as *storage) GetQueueMessage(ctx context.Context, queueName string, leaseDuration int64) (internal.QueueMessageInfo, error) {

	max := strconv.FormatInt(time.Now().UnixMilli(), 10)
	// Acquire a lease for the next message in the queue
	members, err := as.redisClient.ZRangeByScore(ctx, queueName, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    max,
		Offset: 0,
		Count:  10,
	}).Result()
	if err != nil {
		log.Println("Failed to get queue members. Err: ", err)
		return internal.QueueMessageInfo{}, err
	}
	if len(members) == 0 {
		log.Println("No messages found with a deadline before now")
		// No message found with a deadline before now
		return internal.QueueMessageInfo{}, err
	}

	var msg internal.QueueMessageInfo
	// Get the messages metadata from hash and find the next pending message
	for _, member := range members {
		if len(member) == 0 {
			continue
		}
		// Get metadata for given member
		metaResult, err := as.redisClient.HGetAll(ctx, fmt.Sprintf("%s:%s:meta", queueName, member)).Result()
		if err != nil {
			log.Println("Failed to get message's metadata. Err: ", err)
			return internal.QueueMessageInfo{}, err
		}
		// Deserialize the message metadata
		foundMsg, err := internal.UnMarshalRedisMessageInfo(metaResult)
		if err != nil {
			log.Println("Failed to deserialize message's metadata")
			return internal.QueueMessageInfo{}, err
		}
		if foundMsg.State == internal.MESSAGE_PENDING {
			msg = foundMsg
			break
		}
	}
	if msg.MessageID == "" {
		log.Println("No pending messages found with a deadline before now")
		return internal.QueueMessageInfo{}, err
	}

	// Update the message's state to "Running" and restore the message
	msg.State = internal.MESSAGE_RUNNING
	if msg.LeaseDuration <= 0 {
		// Get the default queue's lease duration
		queueMetaResult, err := as.redisClient.HGetAll(ctx, fmt.Sprintf("%s:meta", queueName)).Result()
		if err != nil {
			log.Println("Failed to get queue's metadata. Err: ", err)
			return internal.QueueMessageInfo{}, err
		}
		// convert to a json the message metadata
		queueInfo, err := internal.UnMarshalRedisQueueInfo(queueMetaResult)
		if err != nil {
			log.Println("Failed to get deserialize queue's metadata. Err: ", err)
			return internal.QueueMessageInfo{}, err
		}
		msg.LeaseDuration = queueInfo.LeaseDuration
	}

	// Add lease expiry data to the message metadata
	msg.LeaseExpiry = time.Now().Add(time.Duration(msg.LeaseDuration)).UnixNano() / int64(time.Millisecond)

	err = as.redisClient.HSet(ctx, fmt.Sprintf("%s:%s:meta", queueName, msg.MessageID), msg).Err()
	if err != nil {
		log.Println("Failed to save message's metadata", err)
		return internal.QueueMessageInfo{}, err
	}

	log.Println("Successfully leased the message until: ", msg.LeaseExpiry)
	// Return the deserialized message and lease expiry
	return msg, nil
}

func (as *storage) DeleteQueueMessage(ctx context.Context, queueName string, messageID string) error {
	_, err := as.redisClient.Del(ctx, messageID).Result()
	if err != nil {
		return err
	}
	return nil
}

func (as *storage) AcknowledgeMessage(ctx context.Context, queueName string, messageID string, state internal.State) error {
	// Get metadata for given member
	metaResult, err := as.redisClient.HGetAll(ctx, fmt.Sprintf("%s:%s:meta", queueName, messageID)).Result()
	if err != nil {
		log.Println("Failed to get message's metadata. Err: ", err)
		return err
	}
	// Deserialize the message metadata
	foundMsg, err := internal.UnMarshalRedisMessageInfo(metaResult)
	if err != nil {
		log.Println("Failed to deserialize message's metadata")
		return err
	}
	// Set the message state to passed in state
	foundMsg.State = state
	setResult, err := as.redisClient.HSet(ctx, fmt.Sprintf("%s:%s:meta", queueName, messageID), foundMsg).Result()
	if err != nil {
		log.Println("Failed to get message's metadata. Err: ", err)
		return err
	}
	log.Println("Successfully saved the message metadata: ", setResult)
	return nil
}

func (as *storage) RenewMessageLease(ctx context.Context, queueName string, leaseDuration int64, messageID string) error {
	// Get metadata for given member
	metaResult, err := as.redisClient.HGetAll(ctx, fmt.Sprintf("%s:%s:meta", queueName, messageID)).Result()
	if err != nil {
		log.Println("Failed to get message's metadata. Err: ", err)
		return err
	}
	// Deserialize the message metadata
	leasedMsg, err := internal.UnMarshalRedisMessageInfo(metaResult)
	if err != nil {
		log.Println("Failed to deserialize message's metadata")
		return err
	}

	leasedMsg.LeaseDuration = leaseDuration
	// calculate the new expiry date and it to the message metadata
	leasedMsg.LeaseExpiry = time.Now().Add(time.Duration(leasedMsg.LeaseDuration)).UnixNano() / int64(time.Millisecond)
	setResult, err := as.redisClient.HSet(ctx, fmt.Sprintf("%s:%s:meta", queueName, messageID), leasedMsg).Result()
	if err != nil {
		log.Println("Failed to get message's metadata. Err: ", err)
		return err
	}
	log.Println("Successfully saved the message metadata: ", setResult)
	return nil
}

func (as *storage) PeekQueueMessages(ctx context.Context, queueName string, limit int64, priorityRange internal.PriorityRange) ([]internal.QueueMessageInfo, error) {
	// Get the member IDs of the messages in the sorted set with scores up to the current time.
	min := "-inf"
	max := strconv.FormatInt(time.Now().Unix(), 10)
	if (priorityRange != internal.PriorityRange{}) {
		min = strconv.FormatInt(priorityRange.Min, 10)
		max = strconv.FormatInt(priorityRange.Max, 10)
	}
	memberIDs, err := as.redisClient.ZRangeByScore(ctx, queueName, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: 0,
		Count:  limit,
	}).Result()
	if err != nil {
		return []internal.QueueMessageInfo{}, err
	}

	messages := []internal.QueueMessageInfo{}
	// Get the messages' values using their member IDs.
	for _, memberID := range memberIDs {
		if len(memberID) == 0 {
			continue
		}
		metaResult, err := as.redisClient.HGetAll(ctx, fmt.Sprintf("%s:%s:meta", queueName, memberID)).Result()
		if err != nil {
			log.Println("Failed to serialize message's metadata", err)
			return nil, err
		}
		message, err := internal.UnMarshalRedisMessageInfo(metaResult)
		if err != nil {
			log.Println("Failed to serialize message's metadata", err)
			return nil, err
		}
		messages = append(messages, message)

	}
	return messages, nil
}

func (as *storage) GetQueueState(ctx context.Context, queueName string) (internal.QueueStateInfo, error) {
	membersWithScores, err := as.redisClient.ZRangeByScoreWithScores(ctx, queueName, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: 0,
	}).Result()
	if err != nil {
		return internal.QueueStateInfo{}, err
	}
	// convert timefloat to time.
	earliestDeadline := time.Unix(0, int64(membersWithScores[0].Score)*int64(time.Millisecond))

	invisibleMessagesCount := 0
	pendingMessagesCount := 0
	runningMessagesCount := 0
	completedMessagesCount := 0
	canceledMessagesCount := 0
	erroredMessagesCount := 0

	// Get the messages' values using their member IDs.
	for _, memberID := range membersWithScores {
		metaResult, err := as.redisClient.HGetAll(ctx, fmt.Sprintf("%s:%s:meta", queueName, memberID.Member)).Result()
		if err != nil {
			return internal.QueueStateInfo{}, err
		}
		// Deserialize the message metadata
		message, err := internal.UnMarshalRedisMessageInfo(metaResult)
		if err != nil {
			log.Println("Failed to deserialize message's metadata")
			return internal.QueueStateInfo{}, err
		}
		switch message.State {
		case internal.MESSAGE_INVISIBLE:
			invisibleMessagesCount += 1
		case internal.MESSAGE_PENDING:
			pendingMessagesCount += 1
		case internal.MESSAGE_RUNNING:
			runningMessagesCount += 1
		case internal.MESSAGE_COMPLETED:
			completedMessagesCount += 1
		case internal.MESSAGE_CANCELED:
			canceledMessagesCount += 1
		case internal.MESSAGE_ERRORED:
			erroredMessagesCount += 1
		default:
			continue
		}
	}
	log.Println("====>> Queue State: ",
		"invisibleMessagesCount: ", invisibleMessagesCount,
		"pendingMessagesCount: ", pendingMessagesCount,
		"runningMessagesCount: ", runningMessagesCount,
		"completedMessagesCount: ", completedMessagesCount,
		"canceledMessagesCount: ", canceledMessagesCount,
		"erroredMessagesCount: ", erroredMessagesCount,
		"earliestDeadline: ", earliestDeadline)
	return internal.QueueStateInfo{
		InvisibleMessagesCount: int32(invisibleMessagesCount),
		PendingMessagesCount:   int32(pendingMessagesCount),
		RunningMessagesCount:   int32(runningMessagesCount),
		CompletedMessagesCount: int32(completedMessagesCount),
		CanceledMessagesCount:  int32(canceledMessagesCount),
		ErroredMessagesCount:   int32(erroredMessagesCount),
		EarliestDeadline:       earliestDeadline,
	}, nil
}

func (as *storage) RunLuaScripts(ctx context.Context) {
	// create a ticker to run the script every minute
	ticker := time.NewTicker(1 * time.Minute)

	for {
		// wait for the ticker to fire
		<-ticker.C

		// get the current time in Unix milliseconds
		now := time.Now().UnixNano() / int64(time.Millisecond)

		// run the Lua script with the current time as an argument
		err := invisibleToPending.Run(ctx, as.redisClient, nil, now).Err()
		if err != nil && err.Error() != "redis: nil" {
			log.Printf("Failed to run the script %d\n", err)
			panic(err)
		}
	}
}
