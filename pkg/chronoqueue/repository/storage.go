package repository

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Storage interface {
	CreateQueue(ctx context.Context, request *chronoqueue.CreateQueueRequest) (*chronoqueue.CreateQueueResponse, error)
	DeleteQueue(ctx context.Context, request *chronoqueue.DeleteQueueRequest) (*chronoqueue.DeleteQueueResponse, error)
	CreateQueueMessage(ctx context.Context, request *chronoqueue.PostMessageRequest) (*chronoqueue.PostMessageResponse, error)
	GetQueueMessage(ctx context.Context, request *chronoqueue.GetNextMessageRequest) (*chronoqueue.GetNextMessageResponse, error)
	DeleteQueueMessage(ctx context.Context, queueName string, messageID string) error
	AcknowledgeMessage(ctx context.Context, request *chronoqueue.AcknowledgeMessageRequest) (*chronoqueue.AcknowledgeMessageResponse, error)
	RenewMessageLease(ctx context.Context, request *chronoqueue.RenewMessageLeaseRequest) (*chronoqueue.RenewMessageLeaseResponse, error)
	PeekQueueMessages(ctx context.Context, request *chronoqueue.PeekQueueMessagesRequest) (*chronoqueue.PeekQueueMessagesResponse, error)
	GetQueueState(ctx context.Context, request *chronoqueue.GetQueueStateRequest) (*chronoqueue.GetQueueStateResponse, error)
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

func (as *storage) CreateQueue(ctx context.Context, request *chronoqueue.CreateQueueRequest) (*chronoqueue.CreateQueueResponse, error) {
	txPipeline := as.redisClient.TxPipeline()

	log.Println("RECEIVED QUEUE INFO: ", request.GetQueue())

	queueInfo := request.GetQueue()
	// Check if Queue already exists
	exists, err := txPipeline.Exists(ctx, queueInfo.Name, fmt.Sprintf("%s:meta", queueInfo.GetName())).Result()
	if err != nil {
		log.Println("Failed to check queue existance. err: ", err)
		return &chronoqueue.CreateQueueResponse{}, err
	}
	if exists >= 2 {
		err := errors.New("queue alread exist")
		log.Println("Failed to add message to Queue members. err: ", err)
		return &chronoqueue.CreateQueueResponse{}, err
	}

	createResult, err := txPipeline.ZAdd(ctx, queueInfo.GetName(), redis.Z{}).Result()
	if err != nil {
		log.Println("Failed to create queue. Err: ", err)
		return &chronoqueue.CreateQueueResponse{}, err
	}
	log.Println("Successfully created Queue. result: ", createResult)
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}

	queueMetadataByte, _ := m.Marshal(queueInfo.GetMetadata())
	if err != nil {
		log.Println("Failed to marshal queue's meta. Err: ", err)
		return &chronoqueue.CreateQueueResponse{}, err
	}

	if queueInfo.Metadata.GetType() == chronoqueue.Queue_Options_EXCLUSIVE {
		if queueInfo.Metadata.GetExclusivityKey() == "" {
			return &chronoqueue.CreateQueueResponse{}, errors.New("exclusivity key missing for an EXCLUSIVE queue type")
		} else {
			metaResult, err := txPipeline.HSet(ctx, fmt.Sprintf("%s:meta", queueInfo.GetName()), "metadata", string(queueMetadataByte)).Result()
			if err != nil {
				log.Println("Failed to create exclusive queue's meta. Err: ", err)
				return &chronoqueue.CreateQueueResponse{}, err
			}
			log.Println("Successfully created metadata for Queue. result: ", metaResult)
		}
	} else {
		metaResult, err := txPipeline.HSet(ctx, fmt.Sprintf("%s:meta", queueInfo.GetName()), "metadata", string(queueMetadataByte)).Result()
		if err != nil {
			log.Println("Failed to create non exclusive queue's meta. Err: ", err)
			return &chronoqueue.CreateQueueResponse{}, err
		}
		log.Println("Successfully created metadata for Queue. result: ", metaResult)
	}
	_, err = txPipeline.Exec(ctx)
	if err != nil {
		log.Println("Failed to create queue. error: ", err)
		return &chronoqueue.CreateQueueResponse{}, err
	}
	return &chronoqueue.CreateQueueResponse{}, nil
}

func (as *storage) DeleteQueue(ctx context.Context, request *chronoqueue.DeleteQueueRequest) (*chronoqueue.DeleteQueueResponse, error) {

	checker := NewKeyChecker(as.redisClient, 100)

	start := time.Now()
	checker.Start(ctx)

	iter := as.redisClient.Scan(ctx, 0, fmt.Sprintf("%s*", request.GetName()), 0).Iterator()
	for iter.Next(ctx) {
		checker.Add(iter.Val())
	}
	if err := iter.Err(); err != nil {
		return &chronoqueue.DeleteQueueResponse{}, err
	}

	deleted := checker.Stop()
	log.Println("deleted", deleted, "keys", "in", time.Since(start))

	return &chronoqueue.DeleteQueueResponse{}, nil
}

func (as *storage) CreateQueueMessage(ctx context.Context, request *chronoqueue.PostMessageRequest) (*chronoqueue.PostMessageResponse, error) {

	queueKeys := []string{request.GetQueueName(), fmt.Sprintf("%s:meta", request.GetQueueName())}
	// Check if Queue exists
	exists, err := as.redisClient.Exists(ctx, queueKeys...).Result()
	if err != nil {
		log.Println("Failed to add message to Queue members. err: ", err)
		return &chronoqueue.PostMessageResponse{}, err
	}
	log.Println("Found exists ==> ", exists)
	if exists < 1 {
		err := errors.New("message's queue does not exist")
		log.Println("Failed to add message to Queue members. err: ", err)
		return &chronoqueue.PostMessageResponse{}, err
	}

	message := request.GetMessage()
	if message.GetMetadata().GetInvisibilityDuration() == 0 {
		message.Metadata.State = chronoqueue.Message_Metadata_PENDING
	}

	txPipeline := as.redisClient.TxPipeline()
	// Calculate the message's deadline as a Unix timestamp based on priority
	deadline := time.Now().Add(time.Duration(message.Priority)).UnixNano() / int64(time.Millisecond)
	addMemberResult, err := txPipeline.ZAdd(ctx, request.GetQueueName(), redis.Z{
		Score:  float64(deadline),
		Member: message.MessageId,
	}).Result()
	if err != nil {
		log.Println("Failed to add message to Queue members. err: ", err)
		return &chronoqueue.PostMessageResponse{}, err
	}
	log.Println("Successfully added message to Queue members. result: ", addMemberResult)

	// Create a proto message marshaller
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}

	messageMetadataByte, _ := m.Marshal(message.Metadata)
	if err != nil {
		log.Println("Failed to marshal queue's meta. Err: ", err)
		return &chronoqueue.PostMessageResponse{}, err
	}

	// create metadata for message
	metaResult, err := txPipeline.HSet(ctx, fmt.Sprintf("%s:%s:meta", request.GetQueueName(), message.MessageId), "metadata", string(messageMetadataByte)).Result()
	if err != nil {
		log.Println("Failed to create message's metadata. Err: ", err)
		return &chronoqueue.PostMessageResponse{}, err
	}

	_, err = txPipeline.Exec(ctx)
	if err != nil {
		log.Println("Failed to execute redis pipe command")
		return &chronoqueue.PostMessageResponse{}, err
	}
	log.Println("Successfully created metadata for message. result: ", metaResult)
	return &chronoqueue.PostMessageResponse{}, nil
}

func (as *storage) GetQueueMessage(ctx context.Context, request *chronoqueue.GetNextMessageRequest) (*chronoqueue.GetNextMessageResponse, error) {

	max := strconv.FormatInt(time.Now().UnixMilli(), 10)
	// Acquire a lease for the next message in the queue
	members, err := as.redisClient.ZRangeByScore(ctx, request.GetQueueName(), &redis.ZRangeBy{
		Min:    "-inf",
		Max:    max,
		Offset: 0,
		Count:  10,
	}).Result()
	if err != nil {
		log.Println("Failed to get queue members. Err: ", err)
		return &chronoqueue.GetNextMessageResponse{}, err
	}
	if len(members) == 0 {
		log.Println("No messages found with a deadline before now")
		// No message found with a deadline before now
		return &chronoqueue.GetNextMessageResponse{}, err
	}

	// var msg internal.QueueMessageInfo
	message := chronoqueue.Message{}
	// Get the messages metadata from hash and find the next pending message
	for _, member := range members {
		if len(member) == 0 {
			continue
		}
		// Get metadata for given member
		metaResult, err := as.redisClient.HGet(ctx, fmt.Sprintf("%s:%s:meta", request.GetQueueName(), member), "metadata").Result()
		if err != nil {
			log.Println("Failed to get message's metadata. Err: ", err)
			return &chronoqueue.GetNextMessageResponse{}, err
		}
		// Deserialize the message metadata
		var meta chronoqueue.Message_Metadata
		err = protojson.Unmarshal([]byte(metaResult), &meta)
		if err != nil {
			return &chronoqueue.GetNextMessageResponse{}, err
		}
		if meta.State == chronoqueue.Message_Metadata_PENDING {
			message.MessageId = member
			message.Priority = 0
			message.Metadata = &meta
			break
		}
	}
	if message.MessageId == "" {
		log.Println("No pending messages found with a deadline before now")
		return &chronoqueue.GetNextMessageResponse{}, err
	}

	// Update the message's state to "Running" and restore the message
	message.Metadata.State = chronoqueue.Message_Metadata_RUNNING
	if message.Metadata.GetLeaseDuration() <= 0 {
		// Get the default queue's lease duration
		queueMetaResult, err := as.redisClient.HGet(ctx, fmt.Sprintf("%s:meta", request.GetQueueName()), "metadata").Result()
		if err != nil {
			log.Println("Failed to get queue's metadata. Err: ", err)
			return &chronoqueue.GetNextMessageResponse{}, err
		}
		// convert to a json the message metadata
		var queueMeta chronoqueue.Queue
		err = protojson.Unmarshal([]byte(queueMetaResult), &queueMeta)
		// queueInfo, err := internal.UnMarshalRedisQueueInfo(queueMetaResult)
		if err != nil {
			log.Println("Failed to get deserialize queue's metadata. Err: ", err)
			return &chronoqueue.GetNextMessageResponse{}, err
		}
		message.Metadata.LeaseDuration = &queueMeta.Metadata.LeaseDuration
	}

	// Add lease expiry data to the message metadata
	expireDate := time.Now().Add(time.Duration(message.Metadata.GetLeaseDuration())).UnixNano() / int64(time.Millisecond)
	message.Metadata.LeaseExpiry = &expireDate

	// Create a proto message's metadata marshaller
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	messageMetadataByte, _ := m.Marshal(message.Metadata)
	if err != nil {
		log.Println("Failed to marshal queue's meta. Err: ", err)
		return &chronoqueue.GetNextMessageResponse{}, err
	}

	err = as.redisClient.HSet(ctx, fmt.Sprintf("%s:%s:meta", request.GetQueueName(), message.MessageId), "metadata", string(messageMetadataByte)).Err()
	if err != nil {
		log.Println("Failed to save message's metadata", err)
		return &chronoqueue.GetNextMessageResponse{}, err
	}

	log.Println("Successfully leased the message until: ", message.Metadata.GetLeaseExpiry())
	// Return the deserialized message and lease expiry
	return &chronoqueue.GetNextMessageResponse{
		Message: &message,
	}, nil
}

func (as *storage) DeleteQueueMessage(ctx context.Context, queueName string, messageID string) error {
	_, err := as.redisClient.Del(ctx, messageID).Result()
	if err != nil {
		return err
	}
	return nil
}

func (as *storage) AcknowledgeMessage(ctx context.Context, request *chronoqueue.AcknowledgeMessageRequest) (*chronoqueue.AcknowledgeMessageResponse, error) {
	// Get metadata for given member
	metaResult, err := as.redisClient.HGet(ctx, fmt.Sprintf("%s:%s:meta", request.GetQueueName(), request.GetMessageId()), "metadata").Result()
	if err != nil {
		log.Println("Failed to get message's metadata. Err: ", err)
		return &chronoqueue.AcknowledgeMessageResponse{}, err
	}
	// Deserialize the message metadata
	var meta chronoqueue.Message_Metadata
	err = protojson.Unmarshal([]byte(metaResult), &meta)
	if err != nil {
		return &chronoqueue.AcknowledgeMessageResponse{}, err
	}
	// Set the message state to passed in state
	meta.State = request.State

	// Create a proto message's metadata marshaller
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	messageMetadataByte, _ := m.Marshal(&meta)
	if err != nil {
		log.Println("Failed to marshal queue's meta. Err: ", err)
		return &chronoqueue.AcknowledgeMessageResponse{}, err
	}
	setResult, err := as.redisClient.HSet(ctx, fmt.Sprintf("%s:%s:meta", request.GetQueueName(), request.GetMessageId()), "metadata", string(messageMetadataByte)).Result()
	if err != nil {
		log.Println("Failed to get message's metadata. Err: ", err)
		return &chronoqueue.AcknowledgeMessageResponse{}, err
	}
	log.Println("Successfully saved the message metadata: ", setResult)
	return &chronoqueue.AcknowledgeMessageResponse{}, nil
}

func (as *storage) RenewMessageLease(ctx context.Context, request *chronoqueue.RenewMessageLeaseRequest) (*chronoqueue.RenewMessageLeaseResponse, error) {
	// Get metadata for given member
	metaResult, err := as.redisClient.HGet(ctx, fmt.Sprintf("%s:%s:meta", request.GetQueueName(), request.GetMessageId()), "metadata").Result()
	if err != nil {
		log.Println("Failed to get message's metadata. Err: ", err)
		return &chronoqueue.RenewMessageLeaseResponse{}, err
	}
	// Deserialize the message metadata
	var meta chronoqueue.Message_Metadata
	err = protojson.Unmarshal([]byte(metaResult), &meta)
	if err != nil {
		return &chronoqueue.RenewMessageLeaseResponse{}, err
	}
	leaseDuration := request.GetLeaseDuration()
	meta.LeaseDuration = &leaseDuration

	// calculate the new expiry date and it to the message metadata
	expireDate := time.Now().Add(time.Duration(meta.GetLeaseDuration())).UnixNano() / int64(time.Millisecond)
	meta.LeaseExpiry = &expireDate
	// Create a proto message's metadata marshaller
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	messageMetadataByte, _ := m.Marshal(&meta)
	if err != nil {
		log.Println("Failed to marshal queue's meta. Err: ", err)
		return &chronoqueue.RenewMessageLeaseResponse{}, err
	}
	setResult, err := as.redisClient.HSet(ctx, fmt.Sprintf("%s:%s:meta", request.GetQueueName(), request.GetMessageId()), "metadata", string(messageMetadataByte)).Result()
	if err != nil {
		log.Println("Failed to get message's metadata. Err: ", err)
		return &chronoqueue.RenewMessageLeaseResponse{}, err
	}
	log.Println("Successfully saved the message metadata: ", setResult)
	return &chronoqueue.RenewMessageLeaseResponse{}, err
}

func (as *storage) PeekQueueMessages(ctx context.Context, request *chronoqueue.PeekQueueMessagesRequest) (*chronoqueue.PeekQueueMessagesResponse, error) {
	// Get the member IDs of the messages in the sorted set with scores up to the current time.
	min := "-inf"
	max := strconv.FormatInt(time.Now().Unix(), 10)
	if request.PriorityRange != nil {
		min = strconv.FormatInt(request.PriorityRange.GetMin(), 10)
		max = strconv.FormatInt(request.PriorityRange.GetMax(), 10)
	}
	memberIDs, err := as.redisClient.ZRangeByScore(ctx, request.GetQueueName(), &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: 0,
		Count:  request.GetLimit(),
	}).Result()
	if err != nil {
		return &chronoqueue.PeekQueueMessagesResponse{}, err
	}

	messages := []*chronoqueue.Message{}
	// Get the messages' values using their member IDs.
	for _, memberID := range memberIDs {
		if len(memberID) == 0 {
			continue
		}
		metaResult, err := as.redisClient.HGet(ctx, fmt.Sprintf("%s:%s:meta", request.GetQueueName(), memberID), "metadata").Result()
		if err != nil {
			log.Println("Failed to serialize message's metadata", err)
			return nil, err
		}
		var meta chronoqueue.Message_Metadata
		err = protojson.Unmarshal([]byte(metaResult), &meta)
		if err != nil {
			log.Println("Failed to serialize message's metadata", err)
			return nil, err
		}

		messages = append(messages, &chronoqueue.Message{
			MessageId: memberID,
			Priority:  0,
			Metadata:  &meta,
		})

	}
	return &chronoqueue.PeekQueueMessagesResponse{Messages: messages}, nil
}

func (as *storage) GetQueueState(ctx context.Context, request *chronoqueue.GetQueueStateRequest) (*chronoqueue.GetQueueStateResponse, error) {
	membersWithScores, err := as.redisClient.ZRangeByScoreWithScores(ctx, request.GetQueueName(), &redis.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: 0,
	}).Result()
	if err != nil {
		return &chronoqueue.GetQueueStateResponse{}, err
	}
	// convert timefloat to time.
	// Assumes the first element of array is empty string member
	earliestDeadline := time.Unix(0, int64(membersWithScores[1].Score)*int64(time.Millisecond))

	invisibleMessagesCount := 0
	pendingMessagesCount := 0
	runningMessagesCount := 0
	completedMessagesCount := 0
	canceledMessagesCount := 0
	erroredMessagesCount := 0

	// Get the messages' values using their member IDs.
	for _, memberID := range membersWithScores {
		if len(memberID.Member.(string)) == 0 {
			continue
		}
		metaResult, err := as.redisClient.HGet(ctx, fmt.Sprintf("%s:%s:meta", request.GetQueueName(), memberID.Member), "metadata").Result()
		if err != nil {
			return &chronoqueue.GetQueueStateResponse{}, err
		}
		// Deserialize the message metadata
		var meta chronoqueue.Message_Metadata
		err = protojson.Unmarshal([]byte(metaResult), &meta)
		if err != nil {
			log.Println("Failed to serialize message's metadata", err)
			return &chronoqueue.GetQueueStateResponse{}, err
		}

		switch meta.GetState() {
		case chronoqueue.Message_Metadata_INVISIBLE:
			invisibleMessagesCount += 1
		case chronoqueue.Message_Metadata_PENDING:
			pendingMessagesCount += 1
		case chronoqueue.Message_Metadata_RUNNING:
			runningMessagesCount += 1
		case chronoqueue.Message_Metadata_COMPLETED:
			completedMessagesCount += 1
		case chronoqueue.Message_Metadata_CANCELED:
			canceledMessagesCount += 1
		case chronoqueue.Message_Metadata_ERRORED:
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
	return &chronoqueue.GetQueueStateResponse{
		InvisibleMessagesCount: int32(invisibleMessagesCount),
		PendingMessagesCount:   int32(pendingMessagesCount),
		RunningMessagesCount:   int32(runningMessagesCount),
		CompletedMessagesCount: int32(completedMessagesCount),
		CanceledMessagesCount:  int32(canceledMessagesCount),
		ErroredMessagesCount:   int32(erroredMessagesCount),
		EarliestDeadline:       timestamppb.New(earliestDeadline),
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
