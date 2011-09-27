require 'redis'
require 'zlib'
require 'set'
require File.join(__FILE__,"../server")

class RedCluster
  attr_reader :servers

  def initialize(servers = [])
    @servers = servers.map { |server| Server.new self, server }
  end

  KEY_OPS                   = %W{del exists expire expireat keys move object persists randomkey rename renamenx sort ttl type}.map(&:to_sym)
  SINGLE_KEY_KEY_OPS        = %W{del exists expire expireat move object persists sort ttl type}.map(&:to_sym)

  STRING_OPS                = %W{append decr decrby get getbit getrange getset incr incrby mget mset msetnx set setbit setex setnx setrange strlen}.map(&:to_sym)
  HASH_OPS                  = %W{hdel hexists hget hgetall hincrby hkeys hlen hmget hmset hset hsetnx hvals}.map(&:to_sym)

  LIST_OPS                  = %W{blpop brpop brpoplpush lindex linsert llen lpop lpush lpushx lrange lrem lset ltrim rpop rpoplpush rpush rpushx}.map(&:to_sym)
  SINGLE_KEY_LIST_OPS       = %W{blpop brpop lindex linsert llen lpop lpush lpushx lrange lrem lset ltrim rpop rpush rpushx}.map(&:to_sym)

  SET_OPS                   = %W{sadd scard sdiff sdiffstore sinter sinterstore sismember smembers smove spop srandmember srem sunion sunionstore}.map(&:to_sym)
  SINGLE_KEY_SET_OPS        = %W{sadd scard sismember smembers spop srandmember srem}.map(&:to_sym)

  SORTED_SET_OPS            = %W{zadd zcard zcount zincrby zinterstore zrange zrangebyscore zrank zrem zremrangebyrank zremrangebyscore zrevrange zrevrangebyscore zrevrank zscore zunionstore}.map(&:to_sym)
  SINGLE_KEY_SORTED_SET_OPS = %W{zadd zcard zcount zincrby zrange zrangebyscore zrank zrem zremrangebyrank zremrangebyscore zrevrange zrevrangebyscore zrevrank zscore}.map(&:to_sym)


  SINGLE_KEY_OPS            = SINGLE_KEY_KEY_OPS + STRING_OPS + HASH_OPS + SINGLE_KEY_LIST_OPS + SINGLE_KEY_SET_OPS + SINGLE_KEY_SORTED_SET_OPS

  SERVER_OPS                = %W{multi exec bgsave lastsave flushall flushdb quit ping echo select}.map(&:to_sym)

  def flushdb; @servers.each(&:flushdb); end
  def multi; @servers.each(&:multi); end
  def quit; @servers.each(&:quit); "OK"; end
  def ping; @servers.each(&:ping); "PONG"; end
  def echo(msg); @servers.each {|srvr| srvr.echo(msg) }; msg; end
  def select(db); @servers.each {|srvr| srvr.select(db) }; "OK"; end

  def config(cmd, *args)
    if cmd == :get
      @servers.inject({}) { |result, srvr| result.merge(srvr.config(:get, *args)) }
    else
      @servers.each { |srvr| srvr.config(cmd, *args) }
      "OK"
    end
  end

  def keys(pattern)
    @servers.map do |server|
      server.keys pattern
    end.flatten
  end

  def flushall
    @servers.each { |server| server.flushall }
    "OK"
  end

  def smove(src, destination, member)
    if sismember src, member
      sadd destination, member
      srem src, member
      true
    else
      false
    end
  end

  def exec
    @multi_count = nil
    exec_results = @servers.map(&:exec)
    #We'll get back a deeply nested array of arrays of the kind
    #[[3, 30], [10, 1]], [1, "OK"]] - where the first element in each leaf array is the RANK and the second is the result
    #We need to return back the results sorted by rank. So in the above case it would be
    #["OK", 30, 1]. Ruby's full-LISP toolbox to the rescue
    Hash[*exec_results.flatten].sort.map(&:last)
  end

  def bgsave
    @servers.each(&:bgsave)
    "Background saving started"
  end

  def lastsave
    @servers.map(&:lastsave).sort.first
  end

  def randomkey
    servers_with_keys_in_them  = @servers.select { |server| server.randomkey != nil }
    idx = (rand * servers_with_keys_in_them.count).to_i
    rand_server = servers_with_keys_in_them[idx]
    rand_server && rand_server.randomkey
  end

  def rename(key, new_key)
    raise RuntimeError, "ERR source and destination objects are the same" if key == new_key
    raise RuntimeError, "ERR no such key" unless exists(key)
    val = get key
    del key
    set new_key, val
  end

  def rpoplpush(src_list, target_list)
    val = rpop src_list
    return unless val
    lpush target_list, val
    val
  end

  def brpoplpush(src_list, target_list, timeout)
    val = brpop src_list, timeout
    return unless val
    lpush target_list, val
    val
  end

  def sdiff(*sets)
    perform_set_strategy :difference, *sets
  end

  def sinter(*sets)
    perform_set_strategy :intersection, *sets
  end

  def sunion(*sets)
    perform_set_strategy :union, *sets
  end

  def sinterstore(destination, *sets)
    perform_store_strategy :sinter, destination, *sets
  end

  def sunionstore(destination, *sets)
    perform_store_strategy :sunion, destination, *sets
  end

  def sdiffstore(destination, *sets)
    perform_store_strategy :sdiff, destination, *sets
  end

  def zinterstore(destination, input_sets, options = {})
    perform_sorted_set_store_strategy :intersection, destination, input_sets, options
  end

  def zunionstore(destination, input_sets, options = {})
    perform_sorted_set_store_strategy :union, destination, input_sets, options
  end


  def method_missing(method, *args)
    if SINGLE_KEY_OPS.include?(method.to_sym)
      key = args.first
      server = server_for_key key
      server.send method, *args
    else
      raise "Unsupported operation: #{method}"
    end
  end

  private
  def server_for_key(key)
    @servers[Zlib.crc32(key).abs % @servers.size]
  end

  def multi_count
    @multi_count ||= -1
    @multi_count += 1
  end

  def perform_store_strategy(strategy, destination, *sets)
    del destination
    send(strategy, *sets).each do |entry|
      sadd destination, entry
    end
    scard destination
  end

  def perform_set_strategy(strategy, *sets)
    first_set = Set.new(smembers(sets.first))
    sets[1..-1].inject(first_set) do |accum_set, set|
      accum_set.send(strategy, (Set.new(smembers(set))))
    end.entries
  end

  def perform_sorted_set_store_strategy(strategy, destination, input_sets, options)
    weights = Hash[input_sets.zip(Array(options[:weights]))].reject { |k,v| v == nil }
    first_set = Set.new(zrange(input_sets.first, 0, -1))
    accum_set = input_sets[0..-1].inject(first_set) do |accmltr, set|
      accmltr.send(strategy, Set.new(zrange(set, 0, -1)))
    end
    del destination
    accum_set.entries.each do |entry|
      score_of_input_sets = input_sets.map do |input_set| 
        [input_set, zscore(input_set, entry)] 
      end.reject do |is, zscr|
        zscr == nil 
      end.map do |is,zscr|
        zscr.to_i * weights.fetch(is) { 1 }
      end
      aggregate_function = (options[:aggregate] || :sum)
      score = if aggregate_function == :sum
                score_of_input_sets.inject(0) { |sum, e_score| sum += e_score.to_i }
              elsif [:min, :max].include?(aggregate_function)
                score_of_input_sets.send aggregate_function
              else
                raise "ERR syntax error"
              end
      zadd destination, score, entry
    end
    zcard destination
  end

end

