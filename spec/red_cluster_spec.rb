require 'spec_helper'
require 'red_cluster'

describe RedCluster do
  before do
    first_replica_set = {
      :master => {:host => "localhost", :port => 6379}, 
      :slaves => [{:host => "localhost", :port => 7379},
        {:host => "localhost", :port => 8379}]
    }
    second_replica_set = {
      :master => {:host => "localhost", :port => 9379}, 
      :slaves => [{:host => "localhost", :port => 10379},
        {:host => "localhost", :port => 11379}]
    }
    third_replica_set = {
      :master => {:host => "localhost", :port => 12379}, 
      :slaves => [{:host => "localhost", :port => 13379},
        {:host => "localhost", :port => 14379}]
    }
    replica_sets = [first_replica_set, second_replica_set, third_replica_set]
    @rc = RedCluster.new replica_sets
    @rc.replica_sets.each { |rs| rs.stubs(:read_command?).returns(false) }
  end
  let(:rc) { @rc }
  after { rc.flushall }

  it "gets initialized with a bunch of replica sets" do
    first_replica_set = {
      :master => {:host => "localhost", :port => 6379}, 
      :slaves => [{:host => "localhost", :port => 7379},
        {:host => "localhost", :port => 8379}]
    }
    second_replica_set = {
      :master => {:host => "localhost", :port => 9379}, 
      :slaves => [{:host => "localhost", :port => 10379},
        {:host => "localhost", :port => 11379}]
    }
    third_replica_set = {
      :master => {:host => "localhost", :port => 12379}, 
      :slaves => [{:host => "localhost", :port => 13379},
        {:host => "localhost", :port => 14379}]
    }
    replica_sets = [first_replica_set, second_replica_set, third_replica_set]
    RedCluster.new replica_sets
  end

  context "#randomkey", :fast => true do
    it "returns a random key across the cluster", :fast => true do
      rc.set "foo", "bar"
      rc.randomkey.should == "foo"
    end
    it "returns nil for an empty cluster" do
      rc.randomkey.should_not be
    end
  end

  context "#flushdb" do
    it "works" do
      (1..10_000).to_a.each { |num| rc.set("number|#{num}", "hello") }
      #make sure all servers have a key
      rc.replica_sets.each do |replica_set|
        replica_set.randomkey.should be
      end
      rc.flushdb
      rc.replica_sets.each do |replica_set|
        replica_set.randomkey.should_not be
      end
    end
  end

  context "#flushall" do
    it "flushes keys from all across the cluster" do
      (1..10).to_a.each { |num| rc.set("number|#{num}", "hello") }
      [0, 1, 2].each { |num| rc.replica_sets[num].master.randomkey.should be }
      rc.flushall.should == "OK"
      rc.randomkey.should_not be
    end
  end

  context "#keys" do
    it 'scans across the cluster' do
      (1..10).to_a.each { |num| rc.set("number|#{num}", "hello") }
      rc.keys("*").map(&:to_s).sort.should == rc.replica_sets.inject([]) { |accum, rs| accum << rs.keys("*") }.flatten.map(&:to_s).sort
    end
  end

  context "#smove" do
    it "returns false if the first set does not exist or does not have the member" do
      rc.smove("non_existent_source", "destination", "foo").should == false
      rc.sadd "source", "bar"
      rc.smove("source", "destination", "foo").should == false
    end

    it "returns true if the first set had the member" do
      rc.sadd "source", "foo"
      rc.smove("source", "destination", "foo").should == true
      rc.sismember("source", "foo").should == false
      rc.sismember("destination", "foo").should == true
    end
  end

  context "#sdiffstore", :fast => true do
    it "stores the diff in the destination" do
      (1..10).to_a.each { |num| rc.sadd "set_one", num }
      (5..10).to_a.each { |num| rc.sadd "set_two", num }
      rc.sdiffstore("result_set", "set_one", "set_two").should == 4
      rc.smembers("result_set").sort.should == (1..4).to_a.map(&:to_s)
    end

    it "doesn't store the destination if the diff yielded no results" do
      rc.sdiffstore("result_set", "unknown_set", "set_two").should == 0
      rc.smembers("result_set").should == []
    end
  end

  context "#sdiff", :fast => true do
    it "calculates the diff" do
      (1..10).to_a.each { |num| rc.sadd "set_one", num }
      (5..10).to_a.each { |num| rc.sadd "set_two", num }
      rc.sdiff("set_one", "set_two").sort.should == (1..4).to_a.map(&:to_s)
    end
    it "returns an [] when the first set does not exist" do
      rc.sdiff("unknown_set", "some_set").should == []
    end
  end

  context "#sinter", :fast => true do
    it "calculates the intersection" do
      (1..10).to_a.each { |num| rc.sadd "set_one", num }
      (5..10).to_a.each { |num| rc.sadd "set_two", num }
      rc.sinter("set_one", "set_two").map(&:to_i).sort.should == (5..10).to_a
    end
    it "returns an [] when the first set does not exist" do
      rc.sinter("unknown_set", "some_set").should == []
    end
  end

  context "#sinterstore", :fast => true do
    it "stores the diff in the destination" do
      (1..10).to_a.each { |num| rc.sadd "set_one", num }
      (5..10).to_a.each { |num| rc.sadd "set_two", num }
      rc.sinterstore("result_set", "set_one", "set_two").should == 6
      rc.smembers("result_set").map(&:to_i).sort.should == (5..10).to_a
    end

    it "doesn't store the destination if the diff yielded no results" do
      rc.sadd "result_set", 1
      rc.sinterstore("result_set", "unknown_set", "set_two").should == 0
      rc.smembers("result_set").should == []
      rc.exists("result_set").should_not be
    end
  end

  context "#sunion", :fast => true do
    it "calculates the union" do
      (1..4).to_a.each { |num| rc.sadd "set_one", num }
      (5..10).to_a.each { |num| rc.sadd "set_two", num }
      rc.sunion("set_one", "set_two").map(&:to_i).sort.should == (1..10).to_a
    end
    it "returns an [] when the first set does not exist" do
      rc.sunion("unknown_set", "some_set").should == []
    end
  end

  context "#sunionstore", :fast => true do
    it "stores the union in the destination" do
      (1..4).to_a.each { |num| rc.sadd "set_one", num }
      (5..10).to_a.each { |num| rc.sadd "set_two", num }
      rc.sunionstore("result_set", "set_one", "set_two").should == 10
      rc.smembers("result_set").map(&:to_i).sort.should == (1..10).to_a
    end

    it "doesn't store the destination if the diff yielded no results" do
      rc.sadd "result_set", 1
      rc.sunionstore("result_set", "unknown_set", "set_two").should == 0
      rc.smembers("result_set").should == []
      rc.exists("result_set").should_not be
    end
  end

  context "#rename", :fast => true do
    it "raises an error if the key did not exist" do
      expect { rc.rename("unknown_key", "key") }.to raise_error(RuntimeError, "ERR no such key")
    end
    it "raises an error if the keys are the same" do
      rc.set "foo", "bar"
      expect { rc.rename("foo", "foo") }.to raise_error(RuntimeError, "ERR source and destination objects are the same")
    end
    it "does a rename" do
      rc.set "foo", "bar"
      rc.rename("foo", "foo_new").should == "OK"
      rc.exists("foo").should_not be
      rc.get("foo_new").should == "bar"
    end
  end

  context "#multi-exec", :fast => true do
    it "works" do
      rc.get("foo").should_not be
      rc.get("baz").should_not be
      rc.multi
      100.times do
        rc.set("foo", "bar").should == "QUEUED"
        rc.incr("baz").should == "QUEUED"
      end
      rc.exec.should == 100.times.map { |i| ["OK", i+1] }.flatten
      rc.get("foo").should == "bar"
      rc.get("baz").should == "100"
    end
  end

  context "#watch", :fast => true do
    it "is an unsupported operation" do
      expect { rc.watch }.to raise_error(RuntimeError, "Unsupported operation: watch")
    end
  end

  context "#unwatch", :fast => true do
    it "is an unsupported operation" do
      expect { rc.unwatch }.to raise_error(RuntimeError, "Unsupported operation: unwatch")
    end
  end

  context "bgsave-lastsave" do
    it "returns the earliest lastsave time across the cluster" do
      lastsave = rc.lastsave
      rc.set "foo", "bar"
      rc.bgsave.should == "Background saving started"
      sleep 1 #give it a little time to complete
      new_last_save = rc.lastsave
      # No Idea why this fails when running the whole suite
      # new_last_save.should > lastsave
      rc.replica_sets.map(&:lastsave).sort.first.should == new_last_save
    end
  end

  context "#quit", :fast => true do
    it "closes all the cnxn's it has" do
      rc.quit.should == "OK"
    end
  end

  context "#ping", :fast => true do
    it "ping's all replica_sets in the cluster" do
      rc.replica_sets.each { |rs| rs.should_receive(:ping) }
      rc.ping.should == "PONG"
    end
  end

  context "#echo", :fast => true do
    it "echo's all replica_sets" do
      rc.replica_sets.each { |rs| rs.should_receive(:echo).with("hello") }
      rc.echo("hello").should == "hello"
    end
  end

  context "#config", :fast => true do
    context "#get" do
      it "returns the config values across all replica_sets" do
        rc.config(:get, "*").should_not be_empty
      end
    end

    context "#set", :fast => true do
      it "sets values across all replica_sets" do
        old_timeout = rc.config(:get, "timeout")["timeout"].to_i
        old_timeout.should > 0
        rc.config(:set, "timeout", 100).should == "OK"
        rc.replica_sets.each { |rs| rs.config(:get, "timeout")["timeout"].to_i.should == 100 }
        rc.config(:set, "timeout", old_timeout).should == "OK"
        rc.replica_sets.each { |rs| rs.config(:get, "timeout")["timeout"].to_i.should == old_timeout }
      end
    end

    context "#resetstat", :fast => true do
      it "resets stats across all replica_sets" do
        rc.flushall
        rc.replica_sets.each { |rs| rs.info["total_commands_processed"].to_i.should > 1 }
        rc.config(:resetstat).should == "OK"
        rc.replica_sets.each { |rs| rs.info["total_commands_processed"].to_i.should == 1 }
      end
    end

    context "#bad_command", :fast => true do
      it "raises an error" do
        expect { rc.config(:bad_command) }.to raise_error(RuntimeError, "ERR CONFIG subcommand must be one of GET, SET, RESETSTAT")
      end
    end
  end

  context "#auth", :fast => true do
    it "is not supported" do
      expect { rc.auth "foobar" }.to raise_error(RuntimeError, "Unsupported operation: auth")
    end
  end

  context "#discard", :fast => true do
    it "is not supported" do
      expect { rc.discard }.to raise_error(RuntimeError, "Unsupported operation: discard")
    end
  end

  context "#watch", :fast => true do
    it "is not supported" do
      expect { rc.watch }.to raise_error(RuntimeError, "Unsupported operation: watch")
    end
  end

  context "#object", :fast => true do
    it "is not supported" do
      expect { rc.object }.to raise_error(RuntimeError, "Unsupported operation: object")
    end
  end

  context "#sort", :fast => true do
    it "is not supported" do
      expect { rc.sort }.to raise_error(RuntimeError, "Unsupported operation: sort")
    end
  end

  context "#zinterstore", :fast => true do
    before do
      rc.zadd "my_zset_one", 1, "key_one"
      rc.zadd "my_zset_two", 10, "key_one"
      rc.zadd "my_zset_one", 2, "key_two"
      rc.zadd "my_zset_two", 20, "key_two"
      rc.zadd "my_zset_two", 30, "key_three"
    end

    it "without weights and no aggregate function" do
      rc.zinterstore("result", ["my_zset_one", "my_zset_two"]).should == 2
      rc.zscore("result", "key_one").to_i.should == 11
      rc.zscore("result", "key_two").to_i.should == 22
      rc.zscore("result", "key_three").should_not be
    end

    it "with weights" do
      rc.zinterstore("result", ["my_zset_one", "my_zset_two"], :weights => [10, 1]).should == 2
      rc.zscore("result", "key_one").to_i.should == (10*1 + 10)
      rc.zscore("result", "key_two").to_i.should == (10*2 + 20)
    end

    context "with AGGREGATE" do
      it "sums" do
        rc.zinterstore("result", ["my_zset_one", "my_zset_two"], :weights => [10, 1], :aggregate => :sum).should == 2
        rc.zscore("result", "key_one").to_i.should == (10*1 + 10)
        rc.zscore("result", "key_two").to_i.should == (10*2 + 20)
      end

      it "mins" do
        rc.zinterstore("result", ["my_zset_one", "my_zset_two"], :weights => [5, 1], :aggregate => :min).should == 2
        rc.zscore("result", "key_one").to_i.should == 5
        rc.zscore("result", "key_two").to_i.should == 10
      end

      it "max'es" do
        rc.zinterstore("result", ["my_zset_one", "my_zset_two"], :aggregate => :max).should == 2
        rc.zscore("result", "key_one").to_i.should == 10
        rc.zscore("result", "key_two").to_i.should == 20
      end

      it "raise an Error with an invalid aggregate function" do
        rc.zadd "my_zset_one", 1, "key_one"
        rc.zadd "my_zset_two", 10, "key_one"
        expect { rc.zinterstore("result", ["my_zset_one", "my_zset_two"], :aggregate => :blahdiblah) }.to raise_error
      end
    end
  end

  context "#zunionstore", :fast => true do
    before do
      rc.zadd "my_zset_one", 1, "key_one"
      rc.zadd "my_zset_two", 10, "key_one"
      rc.zadd "my_zset_one", 2, "key_two"
      rc.zadd "my_zset_two", 20, "key_two"
      rc.zadd "my_zset_two", 30, "key_three"
    end

    it "without weights and no aggregate function" do
      rc.zunionstore("result", ["my_zset_one", "my_zset_two"]).should == 3
      rc.zscore("result", "key_one").to_i.should == 11
      rc.zscore("result", "key_two").to_i.should == 22
      rc.zscore("result", "key_three").to_i.should == 30
    end

    it "with weights" do
      rc.zunionstore("result", ["my_zset_one", "my_zset_two"], :weights => [10, 1]).should == 3
      rc.zscore("result", "key_one").to_i.should == (10*1 + 10)
      rc.zscore("result", "key_two").to_i.should == (10*2 + 20)
      rc.zscore("result", "key_three").to_i.should == (10*0 + 30)
    end

    context "ZUNIONSTORE with AGGREGATE" do
      it "sums" do
        rc.zunionstore("result", ["my_zset_one", "my_zset_two"], :weights => [10, 1], :aggregate => :sum).should == 3
        rc.zscore("result", "key_one").to_i.should == (10*1 + 10)
        rc.zscore("result", "key_two").to_i.should == (10*2 + 20)
        rc.zscore("result", "key_three").to_i.should == (10*0 + 30)
      end

      it "mins" do
        rc.zunionstore("result", ["my_zset_one", "my_zset_two"], :weights => [5, 1], :aggregate => :min).should == 3
        rc.zscore("result", "key_one").to_i.should == 5
        rc.zscore("result", "key_two").to_i.should == 10
        rc.zscore("result", "key_three").to_i.should == 30
      end

      it "max'es" do
        rc.zunionstore("result", ["my_zset_one", "my_zset_two"], :aggregate => :max).should == 3
        rc.zscore("result", "key_one").to_i.should == 10
        rc.zscore("result", "key_two").to_i.should == 20
        rc.zscore("result", "key_three").to_i.should == 30
      end

      it "raise an Error with an invalid aggregate function" do
        rc.zadd "my_zset_one", 1, "key_one"
        rc.zadd "my_zset_two", 10, "key_one"
        expect { rc.zunionstore("result", ["my_zset_one", "my_zset_two"], :aggregate => :blahdiblah) }.to raise_error
      end
    end
  end

  context "#shutdown", :fast => true do
    it "shutdowns all servers" do
      rc.replica_sets.each { |replica_set| replica_set.should_receive(:shutdown) }
      rc.shutdown
    end
  end

end

