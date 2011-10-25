class RedCluster
  class ReplicaSet
    attr_reader :slaves, :master

    def initialize(options)
      @master = Redis.new options[:master]
      @slaves = options[:slaves].map { |slave_config| Redis.new slave_config }
      setup_slaves
    end

    def method_missing(command, *args)
      if blocking_command?(command)
        raise "Blocking Commands Not Permitted"
      elsif pub_sub_command?(command)
        raise "Pub Sub Commands Not Permitted"
      elsif slaveof_command?(command)
        raise "Slave Commands Not Permitted"
      elsif command == :shutdown
        @master.shutdown
        @slaves.each(&:shutdown)
      elsif read_command?(command)
        next_slave.send command, *args
      else
        @master.send command, *args
      end
    rescue Errno::ECONNREFUSED
      new_master = @slaves.shift
      raise(NoMaster, "No master in replica set") unless new_master
      @master = new_master
      setup_slaves
      retry
    end

    def next_slave
      ret = @slaves.shift
      @slaves.push ret
      ret
    end

    private

    def setup_slaves
      @slaves.each { |slave| slave.slaveof(@master.client.host, @master.client.port) }
    end

    def slaveof_command?(command)
      command == :slaveof
    end

    def read_command?(command)
      [:dbsize, :exists, :get, :getbit, :getrange, :hexists, :hget, :hgetall, :hkeys, :hlen, :hmget, :hvals, :keys, :lastsave, :lindex, :llen, :mget, :object, :randomkey, :scard, :sismember, :smembers, :srandmember, :strlen, :ttl, :zcard, :zcount, :zrange, :zrangebyscore, :zrank, :zrevrange, :zrevrangebyscore, :zrevrank, :zscore].include?(command)
    end

    def blocking_command?(command)
      [:blpop, :brpop, :brpoplpush].include?(command)
    end

    def pub_sub_command?(command)
      [:psubscribe, :publish, :punsunscribe, :subscribe, :unsubscribe].include?(command)
    end
  end
end

class RedCluster
  class NoMaster < ::Exception
  end
end
