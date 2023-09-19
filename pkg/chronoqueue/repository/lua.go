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
		local invisibilityDuration = metadataJson["invisibilityDuration"]

		if invisibilityDuration ~= nil then
			invisibilityDuration = tonumber(invisibilityDuration)
			if invisibilityDuration < current_time then
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

// var runningToPending = redis.NewScript(`
// local current_time = tonumber(ARGV[1])

// local keys = redis.call('keys', '*')

// for _, key in ipairs(keys) do
//     local key_type = redis.call('TYPE', key)["ok"]
//     if key_type == 'hash' then
//         local metadata = redis.call('hget', key, 'metadata')
//         local metadataJson = cjson.decode(metadata)

//         local leaseExpiry = metadataJson["leaseExpiry"]

//         -- If leaseExpiry exists and has passed the current time
//         if leaseExpiry ~= nil and tonumber(leaseExpiry) < current_time then
//             local state = metadataJson["state"]

//             -- If the current state is RUNNING
//             if state ~= nil and state == "RUNNING" then
//                 metadataJson["state"] = 'PENDING'
//                 local updatedMetadata = cjson.encode(metadataJson)
//                 redis.call('hset', key, 'metadata', updatedMetadata)
//                 redis.log(redis.LOG_NOTICE, "Message with key", key, "updated to PENDING due to lease expiry")
//                 redis.log(redis.LOG_NOTICE, "===== DONE ====")
//             end
//         end
//     end
// end
// `)

var runningToPending = redis.NewScript(`
local current_time = tonumber(ARGV[1])

local keys = redis.call('keys', '*')

for _, key in ipairs(keys) do
    local key_type = redis.call('TYPE', key)["ok"]
    if key_type == 'hash' then
        local metadata = redis.call('hget', key, 'metadata')
        local metadataJson = cjson.decode(metadata)
        
        local leaseExpiry = metadataJson["leaseExpiry"]
        
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
