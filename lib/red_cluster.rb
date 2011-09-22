require 'redis'
require 'zlib'
require 'set'

class RedCluster
  attr_reader :servers

  def initialize(servers = [])
    @servers = servers.map { |server| Server.new server }
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

  SERVER_OPS                = %W{multi exec}.map(&:to_sym)

  def keys(pattern)
    @servers.map do |server|
      server.keys pattern
    end.flatten
  end

  def flushall
    @servers.each { |server| server.flushall }
    "OK"
  end

  def multi
    @servers.map(&:multi)
  end

  def exec
    @servers.map(&:exec)
  end

  def randomkey
    servers_with_keys_in_them  = @servers.select { |server| server.randomkey != nil }
    idx = (rand * servers_with_keys_in_them.count).to_i
    rand_server = servers_with_keys_in_them[idx]
    rand_server && rand_server.randomkey
  end

  def rename(key, new_key)
    raise RuntimeError, "ERR source and destination objects are the same" if key == new_key
    raise RuntimeError, "ERR no such key" unless self.exists(key)
    val = self.get key
    self.del key
    self.set new_key, val
  end

  def rpoplpush(src_list, target_list)
    val = self.rpop src_list
    return unless val
    self.lpush target_list, val
    val
  end

  def brpoplpush(src_list, target_list, timeout)
    val = self.brpop src_list, timeout
    return unless val
    self.lpush target_list, val
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

  def zinterstore(*sorted_sets)
    raise "Operation Not Supported -- Yet!"
  end

  def zunionstore(*sorted_sets)
    raise "Operation Not Supported -- Yet!"
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
    @servers[Zlib.crc32(key) % @servers.size]
  end

  class Server
    attr_reader :host, :port
    def initialize(params = {})
      @host, @port = params[:host], params[:port].to_i
      @redis = Redis.new :host => @host, :port => @port
    end

    def method_missing(method, *args)
      @redis.send method, *args
    end
  end

  def perform_store_strategy(strategy, destination, *sets)
    self.del destination
    self.send(strategy, *sets).each do |entry|
      self.sadd destination, entry
    end
    self.scard destination
  end

  def perform_set_strategy(strategy, *sets)
    first_set = Set.new(self.smembers(sets.first))
    sets[1..-1].inject(first_set) do |inter_set, set|
      inter_set.send(strategy,(Set.new(self.smembers(set))))
    end.entries
  end
end
