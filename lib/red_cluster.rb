require 'redis'
require 'zlib'
require 'set'
require 'replica_set'
require File.join(__FILE__,"../server")

class RedCluster
  attr_reader :replica_sets

  def initialize(replica_sets = [])
    @replica_sets = replica_sets.map { |replica_set| ReplicaSet.new replica_set }
  end

  SINGLE_KEY_KEY_OPS        = %W{del exists expire expireat move object persists sort ttl type}.map(&:to_sym)
  STRING_OPS                = %W{append decr decrby get getbit getrange getset incr incrby mget mset msetnx set setbit setex setnx setrange strlen}.map(&:to_sym)
  HASH_OPS                  = %W{hdel hexists hget hgetall hincrby hkeys hlen hmget hmset hset hsetnx hvals}.map(&:to_sym)
  SINGLE_KEY_LIST_OPS       = %W{blpop brpop lindex linsert llen lpop lpush lpushx lrange lrem lset ltrim rpop rpush rpushx}.map(&:to_sym)
  SINGLE_KEY_SET_OPS        = %W{sadd scard sismember smembers spop srandmember srem}.map(&:to_sym)
  SINGLE_KEY_SORTED_SET_OPS = %W{zadd zcard zcount zincrby zrange zrangebyscore zrank zrem zremrangebyrank zremrangebyscore zrevrange zrevrangebyscore zrevrank zscore}.map(&:to_sym)
  SINGLE_KEY_OPS            = SINGLE_KEY_KEY_OPS + STRING_OPS + HASH_OPS + SINGLE_KEY_LIST_OPS + SINGLE_KEY_SET_OPS + SINGLE_KEY_SORTED_SET_OPS

  # Server Ops
  def select(db); @replica_sets.each {|srvr| srvr.select(db) }; "OK"; end
  def echo(msg); @replica_sets.each {|srvr| srvr.echo(msg) }; msg; end
  def auth(pwd); @replica_sets.each {|srvr| srvr.auth(pwd) }; "OK"; end
  def flushdb; @replica_sets.each(&:flushdb); end
  def shutdown; @replica_sets.each(&:shutdown); end
  def flushall; @replica_sets.each { |server| server.flushall }; "OK"; end
  def quit; @replica_sets.each(&:quit); "OK"; end
  def ping; @replica_sets.each(&:ping); "PONG"; end
  def keys(pattern); @replica_sets.map { |server| server.keys pattern }.flatten; end
  def bgsave; @replica_sets.each(&:bgsave); "Background saving started"; end
  def lastsave; @replica_sets.map(&:lastsave).min; end

  def config(cmd, *args)
    if cmd == :get
      @replica_sets.inject({}) { |result, replica_set| result.merge(srvr.config(:get, *args)) }
    else
      @replica_sets.each { |srvr| srvr.config(cmd, *args) }
      "OK"
    end
  end

  # Transaction Ops
  def multi; @replica_sets.each(&:multi); end

  def exec
    @multi_count = nil
    exec_results = @replica_sets.map(&:exec)
    #We'll get back a deeply nested array of arrays of the kind
    #[[3, 30], [10, 1]], [1, "OK"]] - where the first element in each leaf array is the RANK and the second is the result
    #We need to return back the results sorted by rank. So in the above case it would be
    #["OK", 30, 1]. Ruby's full-LISP toolbox to the rescue
    Hash[*exec_results.flatten].sort.map(&:last)
  end

  def discard
    @multi_count = nil
    @replica_sets.each(&:discard)
    'OK'
  end

  # Key Ops
  def randomkey
    replica_sets_with_keys_in_them  = @replica_sets.select { |replica_set| replica_set.randomkey != nil }
    idx = (rand * replica_sets_with_keys_in_them.count).to_i
    rand_replica_set = replica_sets_with_keys_in_them[idx]
    rand_replica_set && rand_replica_set.randomkey
  end

  def rename(key, new_key)
    raise RuntimeError, "ERR source and destination objects are the same" if key == new_key
    raise RuntimeError, "ERR no such key" unless exists(key)
    val = get key
    del key
    set new_key, val
  end

  # List Ops
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

  # Set Ops
  def smove(src, destination, member)
    if sismember src, member
      sadd destination, member
      srem src, member
      true
    else
      false
    end
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

  # Sorted Set Ops
  def zinterstore(destination, input_sets, options = {})
    perform_sorted_set_store_strategy :intersection, destination, input_sets, options
  end

  def zunionstore(destination, input_sets, options = {})
    perform_sorted_set_store_strategy :union, destination, input_sets, options
  end


  def method_missing(method, *args)
    if SINGLE_KEY_OPS.include?(method.to_sym)
      key = args.first
      replica_set = replica_set_for_key key
      replica_set.send method, *args
    else
      raise "Unsupported operation: #{method}"
    end
  end

  private
  def replica_set_for_key(key)
    @replica_sets[Zlib.crc32(key).abs % @replica_sets.size]
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
    weights = Array(options[:weights])

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
        zscr.to_i * weights.fetch(input_sets.index(is), 1)
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

