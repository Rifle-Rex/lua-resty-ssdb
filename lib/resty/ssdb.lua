-- Copyright (C) 2021 RexGao (Rifle-Rex)
-- Copyright (C) 2013 LazyZhu (lazyzhu.com)
-- Copyright (C) 2013 IdeaWu (ideawu.com)
-- Copyright (C) 2012 Yichun Zhang (agentzh)


local sub = string.sub
local tcp = ngx.socket.tcp
local insert = table.insert
local concat = table.concat
local len = string.len
local null = ngx.null
local pairs = pairs
local unpack = unpack
local setmetatable = setmetatable
local tonumber = tonumber
local error = error
local gmatch = string.gmatch
local remove = table.remove


local _VERSION = '0.03'

local commands = {
    "set",                  "get",                 "del",
    "scan",                 "rscan",               "keys",
    "incr",                 "decr",                "exists",
    "multi_set",            "multi_get",           "multi_del",
    "multi_exists",
    "hset",                 "hget",                "hdel",
    "hscan",                "hrscan",              "hkeys",
    "hincr",                "hdecr",               "hexists",
    "hsize",                "hlist",
    --[[ "multi_hset", ]]   "multi_hget",          "multi_hdel",
    "multi_hexists",        "multi_hsize",
    "zset",                 "zget",                "zdel",
    "zscan",                "zrscan",              "zkeys",
    "zincr",                "zdecr",               "zexists",
    "zsize",                "zlist",
    --[[ "multi_zset", ]]   "multi_zget",          "multi_zdel",
    "multi_zexists",        "multi_zsize"

}

local _M = {}
local mt = { __index = _M }


function _M:new()
    local sock, err = tcp()
    if not sock then
        return nil, err
    end
    local ins = { 
        sock = sock ,
        ['_reqs'] = nil
    }
    setmetatable(ins, mt)
    return ins
end


function _M:set_timeout(timeout)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end


function _M:connect(...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:connect(...)
end


function _M:set_keepalive(...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:setkeepalive(...)
end


function _M:get_reused_times()
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:getreusedtimes()
end


function _M:close()
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:close()
end


function _M:_read_reply()
	local val = {}

	while true do
		-- read block size
		local line, err, partial = self.sock:receive()
		if not line or len(line)==0 then
			-- packet end
			break
		end
		local d_len = tonumber(line)
        
		-- read block data
		local data, err, partial = self.sock:receive(d_len)
		insert(val, data);
		-- ignore the trailing lf/crlf after block data
		local line, err, partial = self.sock:receive()
	end

	local v_num = tonumber(#val)

	if v_num == 1 then
		return val
	else
		remove(val,1)
		return val
	end
end


local function _gen_req(args)
    local req = {}

    for i = 1, #args do
        local arg = args[i]

        if arg then
            insert(req, len(arg))
            insert(req, "\n")
            insert(req, arg)
            insert(req, "\n")
        else
            return nil, 'err'
        end
    end
    insert(req, "\n")

    -- it is faster to do string concatenation on the Lua land
    -- print("request: ", table.concat(req, ""))

    return concat(req, "")
end


function _M:_do_cmd(...)
    local args = {...}

    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local req = _gen_req(args)

    local reqs = self._reqs
    if reqs then
        insert(reqs, req)
        return
    end

    local bytes, err = sock:send(req)
    
    if not bytes then
        return nil, err
    end

    return self:_read_reply()
end


for i = 1, #commands do
    local cmd = commands[i]

    _M[cmd] =
        function (self, ...)
            return self:_do_cmd(cmd, ...)
        end
end


function _M:multi_hset(hashname, ...)
    local args = {...}
    if #args == 1 then
        local t = args[1]
        local array = {}
        for k, v in pairs(t) do
            insert(array, k)
            insert(array, v)
        end
        -- print("key", hashname)
        return _do_cmd(self, "multi_hset", hashname, unpack(array))
    end

    -- backwards compatibility
    return _do_cmd(self, "multi_hset", hashname, ...)
end


function _M:multi_zset(keyname, ...)
    local args = {...}
    if #args == 1 then
        local t = args[1]
        local array = {}
        for k, v in pairs(t) do
            insert(array, k)
            insert(array, v)
        end
        -- print("key", keyname)
        return _do_cmd(self, "multi_zset", keyname, unpack(array))
    end

    -- backwards compatibility
    return _do_cmd(self, "multi_zset", keyname, ...)
end


function _M:init_pipeline()
    self._reqs = {}
end


function _M:cancel_pipeline()
    self._reqs = nil
end


function _M:commit_pipeline()
    local reqs = self._reqs
    if not reqs then
        return nil, "no pipeline"
    end
    
    self._reqs = nil

    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    
    local bytes, err = sock:send(reqs)
    if not bytes then
        return nil, err
    end
    
    local vals = {}
    for i = 1, #reqs do
        local res, err = self:_read_reply()
        if res then
            insert(vals, res)

        elseif res == nil then
            return nil, err

        else
            insert(vals, err)
        end
    end

    return vals
end


function _M.array_to_hash(t)
    local h = {}
    for i = 1, #t, 2 do
        h[t[i]] = t[i + 1]
    end
    return h
end


local class_mt = {
    -- to prevent use of casual module global variables
    __newindex = function (table, key, val)
        error('attempt to write to undeclared variable "' .. key .. '"')
    end
}


function _M.add_commands(...)
    local cmds = {...}
    local newindex = class_mt.__newindex
    class_mt.__newindex = nil
    for i = 1, #cmds do
        local cmd = cmds[i]
        _M[cmd] =
            function (self, ...)
                return _do_cmd(self, cmd, ...)
            end
    end
    class_mt.__newindex = newindex
end


return _M

