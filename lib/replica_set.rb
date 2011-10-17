class RedCluster
  class ReplicaSet
    attr_reader :slaves, :master

    def initialize(options)
      @master = Redis.new options[:master]
      @slaves = options[:slaves].map { |slave_config| Redis.new slave_config }
      @slaves.each { |slave| slave.slaveof(options[:master][:host], options[:master][:port]) }
    end

  end
end

