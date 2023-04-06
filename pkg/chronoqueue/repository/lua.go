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
