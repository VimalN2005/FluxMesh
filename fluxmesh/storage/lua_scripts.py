"""Lua scripts for atomic Redis queue operations."""

DEQUEUE_JOB_LUA = """
local now_ts = ARGV[1]
local in_flight_key = ARGV[2]
local job_prefix = ARGV[3]

for i = 1, #KEYS do
    local job_id = redis.call('RPOP', KEYS[i])
    if job_id then
        local job_key = job_prefix .. job_id
        local job_data = redis.call('GET', job_key)
        if job_data then
            redis.call('SADD', in_flight_key, job_id)
            return {job_id, job_data, KEYS[i]}
        end
    end
end
return nil
"""

PROMOTE_DELAYED_LUA = """
local delayed_key = KEYS[1]
local now_ts = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local job_prefix = ARGV[3]
local q_critical = ARGV[4]
local q_high = ARGV[5]
local q_default = ARGV[6]
local q_low = ARGV[7]

local due_jobs = redis.call('ZRANGEBYSCORE', delayed_key, '-inf', now_ts, 'LIMIT', 0, limit)
local promoted = 0

for i = 1, #due_jobs do
    local job_id = due_jobs[i]
    redis.call('ZREM', delayed_key, job_id)
    local job_key = job_prefix .. job_id
    local job_raw = redis.call('GET', job_key)
    if job_raw then
        local p_match = string.match(job_raw, '"priority"%s*:%s*(%d+)')
        local target_q = q_default
        if p_match == "0" then
            target_q = q_critical
        elseif p_match == "1" then
            target_q = q_high
        elseif p_match == "2" then
            target_q = q_default
        elseif p_match == "3" then
            target_q = q_low
        end
        local updated_raw = string.gsub(job_raw, '"status"%s*:%s*"[^"]+"', '"status": "PENDING"', 1)
        redis.call('SET', job_key, updated_raw)
        redis.call('LPUSH', target_q, job_id)
        promoted = promoted + 1
    end
end
return promoted
"""

REAP_ZOMBIE_WORKER_LUA = """
local in_flight_key = KEYS[1]
local dlq_key = KEYS[2]
local job_prefix = ARGV[1]
local q_critical = ARGV[2]
local q_high = ARGV[3]
local q_default = ARGV[4]
local q_low = ARGV[5]

local jobs = redis.call('SMEMBERS', in_flight_key)
local reaped_ids = {}

for i = 1, #jobs do
    local job_id = jobs[i]
    local job_key = job_prefix .. job_id
    local job_raw = redis.call('GET', job_key)
    if job_raw then
        local attempts = tonumber(string.match(job_raw, '"attempts"%s*:%s*(%d+)')) or 0
        local max_retries = tonumber(string.match(job_raw, '"max_retries"%s*:%s*(%d+)')) or 3
        attempts = attempts + 1
        
        local p_match = string.match(job_raw, '"priority"%s*:%s*(%d+)')
        local target_q = q_default
        if p_match == "0" then target_q = q_critical
        elseif p_match == "1" then target_q = q_high
        elseif p_match == "2" then target_q = q_default
        elseif p_match == "3" then target_q = q_low
        end

        local updated_raw = string.gsub(job_raw, '"attempts"%s*:%s*%d+', '"attempts": ' .. attempts, 1)

        if attempts > max_retries then
            updated_raw = string.gsub(updated_raw, '"status"%s*:%s*"[^"]+"', '"status": "DEAD_LETTER"', 1)
            redis.call('SET', job_key, updated_raw)
            redis.call('LPUSH', dlq_key, job_id)
        else
            updated_raw = string.gsub(updated_raw, '"status"%s*:%s*"[^"]+"', '"status": "PENDING"', 1)
            redis.call('SET', job_key, updated_raw)
            redis.call('LPUSH', target_q, job_id)
        end
        table.insert(reaped_ids, job_id)
    end
end

redis.call('DEL', in_flight_key)
return reaped_ids
"""
