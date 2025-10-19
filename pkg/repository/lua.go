package repository

import (
	"github.com/redis/go-redis/v9"
)

var invisibleToPending = redis.NewScript(`
local function safe_execute()
    local current_time = tonumber(ARGV[1])
    local debug_info = {}
    local processed_count = 0
    local error_count = 0

    -- Use ZRANGEBYSCORE to get expired INVISIBLE messages efficiently
    -- This replaces the O(n) SCAN operation with O(log n + k) sorted set query
    local expired_keys = redis.call('ZRANGEBYSCORE', 'invisible_messages', 0, current_time)

    for _, key in ipairs(expired_keys) do
        local success, err = pcall(function()
            local key_type = redis.call('TYPE', key)["ok"]
            if key_type == 'hash' then
                local metadata = redis.call('hget', key, 'metadata')
                if metadata then
                    local metadataJson = cjson.decode(metadata)
                    local invisibilityExpiry_milliseconds = tonumber(metadataJson["invisibilityExpiry"] or 0)

                    -- Verify the message is still in INVISIBLE state and actually expired
                    if metadataJson["state"] == "INVISIBLE" and invisibilityExpiry_milliseconds <= current_time then
                        -- Add debug info
                        table.insert(debug_info, {
                            key = key,
                            invisibilityExpiry = invisibilityExpiry_milliseconds,
                            currentTime = current_time,
                            expired = true,
                            transition = "INVISIBLE->PENDING"
                        })

                        metadataJson["state"] = 'PENDING'
                        local updatedMetadata = cjson.encode(metadataJson)
                        redis.call('hset', key, 'metadata', updatedMetadata)

                        -- Update state indexes: remove from invisible_messages
                        redis.call('zrem', 'invisible_messages', key)
                        -- PENDING messages don't need indexing as they're handled by queue priority

                        processed_count = processed_count + 1
                    else
                        -- Message state changed or not expired, remove from index for cleanup
                        redis.call('zrem', 'invisible_messages', key)
                        table.insert(debug_info, {
                            key = key,
                            note = "Removed stale index entry",
                            current_state = metadataJson["state"],
                            invisibilityExpiry = invisibilityExpiry_milliseconds
                        })
                    end
                end
            end
        end)

        if not success then
            error_count = error_count + 1
            table.insert(debug_info, {
                key = key,
                error = tostring(err),
                type = "processing_error"
            })
        end
    end

    return {
        success = true,
        processed = processed_count,
        errors = error_count,
        debug_info = debug_info,
        total_keys = #expired_keys,
        optimization = "sorted_set_query"
    }
end

-- Main execution with top-level error handling
local success, result = pcall(safe_execute)
if success then
    return cjson.encode(result)
else
    return cjson.encode({
        success = false,
        error = tostring(result),
        type = "script_error",
        timestamp = tonumber(ARGV[1])
    })
end
`)

var runningToPending = redis.NewScript(`
local function safe_execute()
    local current_time = tonumber(ARGV[1])
    local debug_info = {}
    local processed_count = 0
    local error_count = 0
    local transitions = 0

    -- Buffer time in milliseconds
    local buffer_time_milliseconds = 2000
    local expiry_threshold = current_time - buffer_time_milliseconds

    -- Use ZRANGEBYSCORE to get expired RUNNING messages efficiently
    -- This replaces the O(n) SCAN operation with O(log n + k) sorted set query
    local expired_keys = redis.call('ZRANGEBYSCORE', 'running_messages', 0, expiry_threshold)

    for _, key in ipairs(expired_keys) do
        local success, err = pcall(function()
            local key_type = redis.call('TYPE', key)["ok"]
            if key_type == 'hash' then
                local metadata = redis.call('hget', key, 'metadata')
                if metadata then
                    local metadataJson = cjson.decode(metadata)
                    local leaseExpiryMilliseconds = tonumber(metadataJson["leaseExpiry"] or 0)
                    local expired = (leaseExpiryMilliseconds + buffer_time_milliseconds) < current_time

                    -- Verify the message is still in RUNNING state and actually expired
                    if metadataJson["state"] == "RUNNING" and expired then
                        table.insert(debug_info, {
                            key = key,
                            leaseExpiry = leaseExpiryMilliseconds,
                            currentTime = current_time,
                            expired = expired,
                            attemptsLeft = tonumber(metadataJson["attemptsLeft"])
                        })

                        local attempts_left = tonumber(metadataJson["attemptsLeft"])
                        local max_attempts = tonumber(metadataJson["maxAttempts"])
                        local old_state = metadataJson["state"]

                        -- Handle retry logic with infinite retry support
                        if max_attempts == -1 then
                            -- Infinite retries: just set back to PENDING
                            metadataJson["state"] = 'PENDING'
                        elseif attempts_left and attempts_left > 0 then
                            -- Normal retry: decrement and set to PENDING
                            metadataJson["state"] = 'PENDING'
                            metadataJson["attemptsLeft"] = attempts_left - 1
                        else
                            -- No more retries: mark as ERRORED for DLQ processing
                            metadataJson["state"] = 'ERRORED'
                            -- Add to DLQ processing index for later processing
                            redis.call('zadd', 'dlq_messages', current_time, key)
                        end
                        local updatedMetadata = cjson.encode(metadataJson)
                        redis.call('hset', key, 'metadata', updatedMetadata)

                        -- Update state indexes: remove from running_messages
                        redis.call('zrem', 'running_messages', key)
                        -- PENDING and ERRORED messages don't need indexing

                        transitions = transitions + 1

                        -- Update debug info with transition details
                        debug_info[#debug_info].transition = {
                            from = old_state,
                            to = metadataJson["state"],
                            newAttemptsLeft = metadataJson["attemptsLeft"]
                        }

                        processed_count = processed_count + 1
                    else
                        -- Message state changed or not expired, remove from index for cleanup
                        redis.call('zrem', 'running_messages', key)
                        table.insert(debug_info, {
                            key = key,
                            note = "Removed stale index entry",
                            current_state = metadataJson["state"],
                            leaseExpiry = leaseExpiryMilliseconds,
                            expired = expired
                        })
                    end
                end
            end
        end)

        if not success then
            error_count = error_count + 1
            table.insert(debug_info, {
                key = key,
                error = tostring(err),
                type = "processing_error"
            })
        end
    end

    return {
        success = true,
        processed = processed_count,
        errors = error_count,
        transitions = transitions,
        debug_info = debug_info,
        total_keys = #expired_keys,
        optimization = "sorted_set_query"
    }
end

-- Main execution with top-level error handling
local success, result = pcall(safe_execute)
if success then
    return cjson.encode(result)
else
    return cjson.encode({
        success = false,
        error = tostring(result),
        type = "script_error",
        timestamp = tonumber(ARGV[1])
    })
end
`)

var processErroredMessages = redis.NewScript(`
local function safe_execute()
    local current_time = tonumber(ARGV[1])
    local debug_info = {}
    local processed_count = 0
    local error_count = 0
    local dlq_moves = 0

    -- Get all ERRORED messages that need DLQ processing
    local errored_keys = redis.call('ZRANGEBYSCORE', 'dlq_messages', 0, current_time)

    for _, key in ipairs(errored_keys) do
        local success, err = pcall(function()
            local key_type = redis.call('TYPE', key)["ok"]
            if key_type == 'hash' then
                local metadata = redis.call('hget', key, 'metadata')
                if metadata then
                    local metadataJson = cjson.decode(metadata)

                    -- Verify message is in ERRORED state and eligible for DLQ
                    if metadataJson["state"] == "ERRORED" then
                        local max_attempts = tonumber(metadataJson["maxAttempts"])
                        local attempts_left = tonumber(metadataJson["attemptsLeft"])

                        -- Only move to DLQ if retries are exhausted and max_attempts is not infinite
                        if max_attempts ~= -1 and attempts_left == 0 then
                            -- Extract queue name from key pattern: queueName:messageId:meta
                            local queue_name = string.match(key, "^([^:]+):")

                            if queue_name then
                                -- Get queue metadata to check DLQ configuration
                                local queue_meta_key = "queue:" .. queue_name .. ":meta"
                                local queue_metadata = redis.call('hget', queue_meta_key, 'metadata')

                                if queue_metadata then
                                    local queueMetaJson = cjson.decode(queue_metadata)

                                    -- Determine DLQ name
                                    local dlq_name = queueMetaJson["deadLetterQueueName"]
                                    if not dlq_name or dlq_name == "" then
                                        dlq_name = queue_name .. "_dlq"
                                    end

                                    -- Check if auto_create_dlq is enabled
                                    local auto_create_dlq = queueMetaJson["autoCreateDlq"]
                                    if auto_create_dlq then
                                        -- Check if DLQ exists
                                        local dlq_meta_key = "queue:" .. dlq_name .. ":meta"
                                        local dlq_exists = redis.call('exists', dlq_meta_key)

                                        if dlq_exists == 0 then
                                            -- Auto-create DLQ with similar settings
                                            local dlq_metadata = {
                                                type = queueMetaJson["type"],
                                                defaultMaxAttempts = 1,
                                                leaseDuration = queueMetaJson["leaseDuration"],
                                                exclusivityKey = queueMetaJson["exclusivityKey"],
                                                invisibilityDuration = queueMetaJson["invisibilityDuration"],
                                                deadLetterQueueName = "",
                                                autoCreateDlq = false
                                            }

                                            local dlq_metadata_str = cjson.encode(dlq_metadata)
                                            redis.call('hset', dlq_meta_key, 'metadata', dlq_metadata_str)

                                            table.insert(debug_info, {
                                                action = "auto_created_dlq",
                                                dlq_name = dlq_name,
                                                original_queue = queue_name
                                            })
                                        end

                                        -- Extract message ID from key
                                        local message_id = string.match(key, "^[^:]+:([^:]+):meta$")

                                        if message_id then
                                            -- Prepare DLQ message metadata
                                            local dlq_metadata = {
                                                payload = metadataJson["payload"],
                                                state = "PENDING",
                                                invisibilityDuration = "0s",
                                                attemptsLeft = 1,
                                                maxAttempts = 1,
                                                leaseDuration = metadataJson["leaseDuration"],
                                                leaseExpiry = 0,
                                                leaseRenewalCount = 0,
                                                invisibilityExpiry = 0,
                                                priority = metadataJson["priority"]
                                            }

                                            -- Calculate DLQ priority score
                                            local max_priority = 1000
                                            local priority_component = (max_priority - tonumber(dlq_metadata.priority or 0)) * 1000000000000
                                            local timestamp_component = current_time
                                            local dlq_score = priority_component + timestamp_component

                                            -- Create DLQ message
                                            local dlq_message_key = dlq_name .. ":" .. message_id .. ":meta"
                                            local dlq_metadata_str = cjson.encode(dlq_metadata)
                                            redis.call('hset', dlq_message_key, 'metadata', dlq_metadata_str)

                                            -- Add to DLQ queue
                                            local prefixed_dlq_name = "queue:" .. dlq_name
                                            redis.call('zadd', prefixed_dlq_name, dlq_score, message_id)

                                            -- Remove from original queue
                                            local prefixed_queue_name = "queue:" .. queue_name
                                            redis.call('zrem', prefixed_queue_name, message_id)

                                            -- Remove original message metadata
                                            redis.call('del', key)

                                            -- Remove from DLQ processing index
                                            redis.call('zrem', 'dlq_messages', key)

                                            dlq_moves = dlq_moves + 1

                                            table.insert(debug_info, {
                                                action = "moved_to_dlq",
                                                original_queue = queue_name,
                                                dlq_name = dlq_name,
                                                message_id = message_id,
                                                key = key,
                                                dlq_key = dlq_message_key
                                            })
                                        end
                                    else
                                        -- Auto-create DLQ disabled, just remove from processing index
                                        redis.call('zrem', 'dlq_messages', key)
                                        table.insert(debug_info, {
                                            action = "skipped_dlq_disabled",
                                            queue_name = queue_name,
                                            key = key
                                        })
                                    end
                                end
                            end
                        else
                            -- Remove from DLQ processing index (not eligible for DLQ)
                            redis.call('zrem', 'dlq_messages', key)
                            table.insert(debug_info, {
                                action = "not_eligible_for_dlq",
                                max_attempts = max_attempts,
                                attempts_left = attempts_left,
                                key = key
                            })
                        end

                        processed_count = processed_count + 1
                    else
                        -- Message state changed, remove from index
                        redis.call('zrem', 'dlq_messages', key)
                        table.insert(debug_info, {
                            action = "state_changed",
                            current_state = metadataJson["state"],
                            key = key
                        })
                    end
                end
            end
        end)

        if not success then
            error_count = error_count + 1
            table.insert(debug_info, {
                key = key,
                error = tostring(err),
                type = "processing_error"
            })
        end
    end

    return {
        success = true,
        processed = processed_count,
        errors = error_count,
        dlq_moves = dlq_moves,
        debug_info = debug_info,
        total_keys = #errored_keys,
        timestamp = current_time
    }
end

-- Main execution with top-level error handling
local success, result = pcall(safe_execute)
if success then
    return cjson.encode(result)
else
    return cjson.encode({
        success = false,
        error = tostring(result),
        type = "script_error",
        timestamp = tonumber(ARGV[1])
    })
end
`)
