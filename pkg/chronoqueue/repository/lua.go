package repository

import (
	"github.com/redis/go-redis/v9"
)

var invisibleToPending = redis.NewScript(`
local current_time = tonumber(ARGV[1])

local keys = redis.call('keys', '*')

for _, key in ipairs(keys) do
    local key_type = redis.call('TYPE', key)["ok"]
    if key_type == 'hash' then
        local metadata = redis.call('hget', key, 'metadata')
        local metadataJson = cjson.decode(metadata)
        
        -- Convert invisibility_duration (seconds + nanos) to milliseconds
        local invisibility_duration_milliseconds = 0
        if metadataJson["invisibilityDuration"] ~= nil then
            local seconds = tonumber(metadataJson["invisibilityDuration"]["seconds"] or 0)
            local nanos = tonumber(metadataJson["invisibilityDuration"]["nanos"] or 0)
            invisibility_duration_milliseconds = (seconds * 1000) + (nanos / 1000000)  -- Convert nanos to milliseconds
        end

        if invisibility_duration_milliseconds ~= nil then
            if invisibility_duration_milliseconds < current_time then
                local state = metadataJson["state"]
                if state ~= nil and state == "INVISIBLE" then
                    metadataJson["state"] = 'PENDING'
                    local updatedMetadata = cjson.encode(metadataJson)
                    redis.call('hset', key, 'metadata', updatedMetadata)
                    redis.log(redis.LOG_NOTICE, "Message with key", key, "updated to PENDING")
                    redis.log(redis.LOG_NOTICE, "===== DONE ====")
                end
            end
        end
    end
end
`)

var runningToPending = redis.NewScript(`
local current_time = tonumber(ARGV[1])

local keys = redis.call('keys', '*')

for _, key in ipairs(keys) do
    local key_type = redis.call('TYPE', key)["ok"]
    if key_type == 'hash' then
        local metadata = redis.call('hget', key, 'metadata')
        local metadataJson = cjson.decode(metadata)
        
        local leaseExpiry = metadataJson["leaseExpiry"]
        
        -- Convert lease_duration (seconds + nanos) to milliseconds
        local lease_duration_milliseconds = 0
        if metadataJson["leaseDuration"] ~= nil then
            local seconds = tonumber(metadataJson["leaseDuration"]["seconds"] or 0)
            local nanos = tonumber(metadataJson["leaseDuration"]["nanos"] or 0)
            lease_duration_milliseconds = (seconds * 1000) + (nanos / 1000000) -- Convert nanos to milliseconds
        end

        -- If leaseExpiry exists and has passed the current time
        if leaseExpiry ~= nil and tonumber(leaseExpiry) < current_time then
            local state = metadataJson["state"]
            
            -- If the current state is RUNNING
            if state ~= nil and state == "RUNNING" then
                -- Check if attempts_left is zero
                local attempts_left = metadataJson["attemptsLeft"]
                if attempts_left ~= nil and tonumber(attempts_left) > 0 then
                    metadataJson["state"] = 'PENDING'
                    metadataJson["attemptsLeft"] = tonumber(attempts_left) - 1
                elseif attempts_left ~= nil and tonumber(attempts_left) == 0 then
                    metadataJson["state"] = 'ERRORED'
                end

                local updatedMetadata = cjson.encode(metadataJson)
                redis.call('hset', key, 'metadata', updatedMetadata)
                redis.log(redis.LOG_NOTICE, "Message with key", key, "updated its state due to lease expiry")
                redis.log(redis.LOG_NOTICE, "===== DONE ====")
            end
        end
    end
end
`)
