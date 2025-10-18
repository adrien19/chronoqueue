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
                        local old_state = metadataJson["state"]
                        if attempts_left and attempts_left > 0 then
                            metadataJson["state"] = 'PENDING'
                            metadataJson["attemptsLeft"] = attempts_left - 1
                        elseif attempts_left == 0 then
                            metadataJson["state"] = 'ERRORED'
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
