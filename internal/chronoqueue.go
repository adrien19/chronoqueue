package internal

import (
	"encoding/json"
	"log"
	"strconv"
	"time"
)

type QueueInfo struct {
	QueueName            string        `json:"queueName" redis:"queueName"`
	PriorityRange        PriorityRange `json:"priorityRange" redis:"priorityRange"`
	QueueType            QueueType     `json:"queueType" redis:"queueType"`
	Attempts             int32         `json:"attempts,string" redis:"attempts"`
	LeaseDuration        int64         `json:"leaseDuration,string" redis:"leaseDuration"`
	ExclusivityKey       string        `json:"exclusivityKey,omitempty" redis:"exclusivityKey,omitempty"`
	InvisibilityDuration int64         `json:"invisibilityDuration,string,omitempty" redis:"invisibilityDuration,omitempty"`
}

func (q QueueInfo) MarshalBinary() ([]byte, error) {
	return json.Marshal(q)
}

func UnMarshalRedisQueueInfo(data map[string]string) (QueueInfo, error) {
	var tempMap map[string]interface{}
	var priorityRange PriorityRange

	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Println("Failed to serialize queue's metadata")
		return QueueInfo{}, err
	}

	if err := json.Unmarshal(jsonData, &tempMap); err != nil {
		log.Println("Failed to deserialize queue's metadata")
		return QueueInfo{}, err
	}
	if tempPriorityRange, ok := tempMap["priorityRange"]; ok {
		if err := json.Unmarshal([]byte(tempPriorityRange.(string)), &priorityRange); err != nil {
			log.Println("Failed to deserialize queue's priority range from metadata")
			return QueueInfo{}, err
		}
	}
	if len(tempMap) != 0 {
		queueName := tempMap["queueName"].(string)
		queueType := QueueType(tempMap["queueType"].(string))
		attempts, _ := strconv.ParseInt(tempMap["attempts"].(string), 10, 0)
		leaseDuration, _ := strconv.ParseInt(tempMap["leaseDuration"].(string), 10, 0)
		exclusivityKey := tempMap["exclusivityKey"].(string)
		invisibilityDuration, _ := strconv.ParseInt(tempMap["invisibilityDuration"].(string), 10, 0)
		return QueueInfo{
			QueueName:            queueName,
			QueueType:            queueType,
			Attempts:             int32(attempts),
			LeaseDuration:        leaseDuration,
			ExclusivityKey:       exclusivityKey,
			InvisibilityDuration: invisibilityDuration,
		}, nil
	}
	return QueueInfo{}, nil
}

type QueueMessageInfo struct {
	MessageID            string  `json:"messageId" redis:"messageId"`
	Payload              Payload `json:"payload" protobuf:"bytes,1,rep,name=fields,proto3" redis:"payload"`
	Priority             int64   `json:"priority" redis:"priority"`
	State                State   `json:"state" redis:"state"`
	InvisibilityDuration int64   `json:"invisibilityDuration,omitempty" redis:"invisibilityDuration,omitempty"`
	AttemptsLeft         int32   `json:"attemptsLeft" redis:"attemptsLeft"`
	LeaseDuration        int64   `json:"leaseDuration,omitempty" redis:"leaseDuration,omitempty"`
	LeaseExpiry          int64   `json:"leaseExpiry,omitempty" redis:"leaseExpiry,omitempty"`
}

func (m QueueMessageInfo) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

func UnMarshalRedisMessageInfo(data map[string]string) (QueueMessageInfo, error) {
	var tempMap map[string]interface{}
	var payload map[string]interface{}
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Println("Failed to serialize message's metadata")
		return QueueMessageInfo{}, err
	}

	if err := json.Unmarshal(jsonData, &tempMap); err != nil {
		log.Println("Failed to deserialize message's metadata")
		return QueueMessageInfo{}, err
	}
	if tempPayload, ok := tempMap["payload"]; ok {
		if err := json.Unmarshal([]byte(tempPayload.(string)), &payload); err != nil {
			log.Println("Failed to deserialize message's payload from metadata")
			return QueueMessageInfo{}, err
		}
	}
	if len(tempMap) != 0 {
		priority, _ := strconv.ParseInt(tempMap["priority"].(string), 10, 0)
		state, _ := strconv.ParseInt(tempMap["state"].(string), 10, 0)
		invisibilityDuration, _ := strconv.ParseInt(tempMap["invisibilityDuration"].(string), 10, 0)
		attemptsLeft, _ := strconv.ParseInt(tempMap["attemptsLeft"].(string), 10, 0)
		leaseDuration, _ := strconv.ParseInt(tempMap["leaseDuration"].(string), 10, 0)
		leaseExpiry, _ := strconv.ParseInt(tempMap["leaseExpiry"].(string), 10, 0)
		return QueueMessageInfo{
			MessageID:            tempMap["messageId"].(string),
			Payload:              payload,
			Priority:             priority,
			State:                State(int32(state)),
			InvisibilityDuration: invisibilityDuration,
			AttemptsLeft:         int32(attemptsLeft),
			LeaseDuration:        leaseDuration,
			LeaseExpiry:          leaseExpiry,
		}, nil

		// log.Println("Formilated message ==>>> ", m)
		// return nil
	}
	return QueueMessageInfo{}, nil
}

type Payload map[string]interface{}

func (pl Payload) MarshalBinary() ([]byte, error) {
	return json.Marshal(pl)
}

type PriorityRange struct {
	Min int64 `json:"min,string" redis:"min"`
	Max int64 `json:"max,string" redis:"max"`
}

func (p PriorityRange) MarshalBinary() ([]byte, error) {
	return json.Marshal(p)
}

type State int32

const (
	state_undefined State = iota
	MESSAGE_INVISIBLE
	MESSAGE_PENDING
	MESSAGE_RUNNING
	MESSAGE_COMPLETED
	MESSAGE_CANCELED
	MESSAGE_ERRORED
)

func (s State) MarshalBinary() ([]byte, error) {
	return json.Marshal(s)
}

type QueueType string

const (
	Simple    QueueType = "SIMPLE"
	Exclusive QueueType = "EXCLUSIVE"
)

func (t QueueType) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

type ErrorCode int32

const (
	UNKNOWN_ERROR ErrorCode = iota
	INVALID_ARGUMENT
	NOT_FOUND
	PERMISSION_DENIED
	UNAUTHENTICATED
	INTERNAL_ERROR
	UNAVAILABLE
	NOT_FOUND_CUSTOM
)

type QueueStateInfo struct {
	InvisibleMessagesCount int32     `json:"invisibleMessagesCount"`
	PendingMessagesCount   int32     `json:"pendingMessagesCount"`
	RunningMessagesCount   int32     `json:"runningMessagesCount"`
	CompletedMessagesCount int32     `json:"completedMessagesCount"`
	CanceledMessagesCount  int32     `json:"canceledMessagesCount"`
	ErroredMessagesCount   int32     `json:"erroredMessagesCount"`
	EarliestDeadline       time.Time `json:"earliestDeadline"`
}
