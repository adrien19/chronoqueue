package repository

import (
	"github.com/redis/go-redis/v9"
)

var invisibleToPending = redis.NewScript(`
local current_time = tonumber(ARGV[1])

local keys = redis.call('keys', '*')

for _, key in ipairs(keys) do
	redis.log(redis.LOG_NOTICE, "This is the key ===> ", key)
	local key_type = redis.call('TYPE', key)["ok"]

	redis.log(redis.LOG_NOTICE, "This is the keyType ===> ", key_type)

	if key_type == 'hash' then
		local invisibilityDuration = redis.call('hget', key, 'invisibilityDuration')
		
		if invisibilityDuration ~= nil then
			invisibilityDuration = tonumber(invisibilityDuration)
			redis.log(redis.LOG_NOTICE, "invisibilityDuration ===> ", invisibilityDuration)

			if invisibilityDuration < current_time then
				local fieldname = 'state'
				local state = redis.call('HGET', key, fieldname)
				if state ~= nil and state == '1' then
					redis.log(redis.LOG_NOTICE, "Setting state to ===> PENDING for Key: ", key)
					redis.call('hset', key, 'state', '2')
					redis.log(redis.LOG_NOTICE, "===== DONE ====")
				end
			end
		end
	else
		redis.log(redis.LOG_NOTICE, "Key is not of type HASH")
	end
end
`)
