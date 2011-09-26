class RedCluster
  class Server
    attr_reader :host, :port

    def initialize(cluster, params = {})
      @host, @port = params[:host], params[:port].to_i
      @redis = Redis.new :host => @host, :port => @port
      @my_cluster = cluster
    end

    def multi
      @in_multi = true
      @cmd_order_in_multi = []
      @redis.multi
    end

    def exec
      @in_multi = nil
      @redis.exec.map { |result| [@cmd_order_in_multi.shift, result] }
    end

    def method_missing(method, *args)
      if @in_multi
        @cmd_order_in_multi << @my_cluster.send(:multi_count)
      end
      @redis.send method, *args
    end
  end
end
