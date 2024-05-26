package repository

import (
	"github.com/redis/go-redis/v9"
)

var invisibleToPending = redis.NewScript(`
local current_time = tonumber(ARGV[1])

-- Use SCAN to incrementally get all keys
local cursor = "0"
local keys = {}

repeat
    local result = redis.call('SCAN', cursor, 'MATCH', '*:*:meta')
    cursor = result[1]
    for i, key in ipairs(result[2]) do
        table.insert(keys, key)
    end
until cursor == "0"

for _, key in ipairs(keys) do
    local key_type = redis.call('TYPE', key)["ok"]
    if key_type == 'hash' then
        local metadata = redis.call('hget', key, 'metadata')
        if metadata and string.match(metadata, "\"state\":\"INVISIBLE\"") then
            local metadataJson = cjson.decode(metadata)
            local invisibilityExpiry_milliseconds = tonumber(metadataJson["invisibilityExpiry"] or 0)
            if invisibilityExpiry_milliseconds < current_time then
                metadataJson["state"] = 'PENDING'
                local updatedMetadata = cjson.encode(metadataJson)
                redis.call('hset', key, 'metadata', updatedMetadata)
            end
        end
    end
end
`)

var runningToPending = redis.NewScript(`
local current_time = tonumber(ARGV[1])

-- Use SCAN to incrementally get all keys
local cursor = "0"
local keys = {}

repeat
    local result = redis.call('SCAN', cursor, 'MATCH', '*:*:meta')
    cursor = result[1]
    for i, key in ipairs(result[2]) do
        table.insert(keys, key)
    end
until cursor == "0"

for _, key in ipairs(keys) do
    local key_type = redis.call('TYPE', key)["ok"]
    if key_type == 'hash' then
        local metadata = redis.call('hget', key, 'metadata')
        if metadata and string.match(metadata, "\"state\":\"RUNNING\"") then
            local metadataJson = cjson.decode(metadata)
            local leaseExpiryMilliseconds = tonumber(metadataJson["leaseExpiry"] or 0)
            -- Buffer time in milliseconds
            local buffer_time_milliseconds = 2000
            if (leaseExpiryMilliseconds + buffer_time_milliseconds) < current_time then
                local attempts_left = tonumber(metadataJson["attemptsLeft"])
                if attempts_left and attempts_left > 0 then
                    metadataJson["state"] = 'PENDING'
                    metadataJson["attemptsLeft"] = attempts_left - 1
                elseif attempts_left == 0 then
                    metadataJson["state"] = 'ERRORED'
                end
                local updatedMetadata = cjson.encode(metadataJson)
                redis.call('hset', key, 'metadata', updatedMetadata)
            end
        end
    end
end
`)
