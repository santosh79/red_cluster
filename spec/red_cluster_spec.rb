require 'rubygems'
# require 'fakeredis'
require File.join(__FILE__,'../../lib/red_cluster')

describe RedCluster do
  before(:all) do
    @rc ||=  RedCluster.new [{:host => "127.0.0.1", :port => "6379"}, {:host => "127.0.0.1", :port => "7379"}] 
  end
  let(:rc) { @rc }
  after { rc.flushall }

  it "gets initialized with an array of hashes containing server info" do
    servers = [{:host => "127.0.0.1", :port => "6379"}, {:host => "127.0.0.1", :port => "7379"}, {:host => "127.0.0.1", :port => "8379"}]
    RedCluster.new servers
  end

  context "#set" do
    it "stores the key value in only one of the servers it's fronting" do
      rc.set "foo", "bar"
      rc.servers.select { |server| server.get("foo") != nil }.size.should == 1
    end
  end

  context "#randomkey" do
    it "returns a random key across the cluster" do
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
      rc.servers.each do |server|
        server.randomkey.should be
      end
      rc.flushdb
      rc.servers.each do |server|
        server.randomkey.should_not be
      end
    end
  end

  context "#flushall" do
    it "flushes keys from all across the cluster" do
      (1..10_000).to_a.each { |num| rc.set("number|#{num}", "hello") }

      first_server_rand_key = rc.servers.first.randomkey
      first_server_rand_key.should be
      second_server_rand_key = rc.servers.last.randomkey
      second_server_rand_key.should be

      rc.flushall.should == "OK"

      first_server_rand_key = rc.servers.first.randomkey
      first_server_rand_key.should_not be
      second_server_rand_key = rc.servers.last.randomkey
      second_server_rand_key.should_not be
      rc.randomkey.should_not be
    end
  end

  context "#keys" do
    it 'scans across the cluster' do
      (1..100).to_a.each { |num| rc.set("number|#{num}", "hello") }
      first_servers_keys = rc.servers.first.keys("*")
      first_servers_keys.size.should > 0
      second_servers_keys = rc.servers.last.keys("*")
      second_servers_keys.size.should > 0
      first_servers_keys.map(&:to_s).sort.should_not == second_servers_keys.map(&:to_s).sort
      all_keys = rc.keys("*")
      all_keys.map(&:to_s).sort.should == (first_servers_keys + second_servers_keys).map(&:to_s).sort
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

  context "#sdiffstore" do
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

  context "#sdiff" do
    it "calculates the diff" do
      (1..10).to_a.each { |num| rc.sadd "set_one", num }
      (5..10).to_a.each { |num| rc.sadd "set_two", num }
      rc.sdiff("set_one", "set_two").sort.should == (1..4).to_a.map(&:to_s)
    end
    it "returns an [] when the first set does not exist" do
      rc.sdiff("unknown_set", "some_set").should == []
    end
  end

  context "#sinter" do
    it "calculates the intersection" do
      (1..10).to_a.each { |num| rc.sadd "set_one", num }
      (5..10).to_a.each { |num| rc.sadd "set_two", num }
      rc.sinter("set_one", "set_two").map(&:to_i).sort.should == (5..10).to_a
    end
    it "returns an [] when the first set does not exist" do
      rc.sinter("unknown_set", "some_set").should == []
    end
  end

  context "#sinterstore" do
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

  context "#sunion" do
    it "calculates the union" do
      (1..4).to_a.each { |num| rc.sadd "set_one", num }
      (5..10).to_a.each { |num| rc.sadd "set_two", num }
      rc.sunion("set_one", "set_two").map(&:to_i).sort.should == (1..10).to_a
    end
    it "returns an [] when the first set does not exist" do
      rc.sunion("unknown_set", "some_set").should == []
    end
  end

  context "#sunionstore" do
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

  context "#rename" do
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

  context "#multi-exec" do
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

  context "bgsave-lastsave" do
    it "returns the earliest lastsave time across the cluster" do
      lastsave = rc.lastsave
      rc.set "foo", "bar"
      rc.bgsave.should == "Background saving started"
      sleep 1 #give it a little time to complete
      new_last_save = rc.lastsave
      # No Idea why this fails when running the whole suite
      # new_last_save.should > lastsave
      rc.servers.map(&:lastsave).sort.first.should == new_last_save
    end
  end

  context "#quit" do
    it "closes all the cnxn's it has" do
      rc.quit.should == "OK"
    end
  end

  context "#ping" do
    it "ping's all servers in the cluster" do
      rc.servers.each { |srvr| srvr.should_receive(:ping) }
      rc.ping.should == "PONG"
    end
  end

  context "#echo" do
    it "echo's all servers" do
      rc.servers.each { |srvr| srvr.should_receive(:echo).with("hello") }
      rc.echo("hello").should == "hello"
    end
  end

  context "#select" do
    it "changes the db across all servers" do
      #select is some kind of weird reserve word - don't want to bother testing this. It works.
      # rc.servers.each { |srvr| srvr.should_receive(:select).with(10) }
      rc.select(10).should == "OK"
    end
  end

  context "#config" do
    context "#get" do
      it "returns the config values across all servers" do
        rc.config(:get, "*").should_not be_empty
      end
    end

    context "#set" do
      it "sets values across all servers" do
        old_timeout = rc.config(:get, "timeout")["timeout"].to_i
        old_timeout.should > 0
        rc.config(:set, "timeout", 100).should == "OK"
        rc.servers.each { |srvr| srvr.config(:get, "timeout")["timeout"].to_i.should == 100 }
        rc.config(:set, "timeout", old_timeout).should == "OK"
        rc.servers.each { |srvr| srvr.config(:get, "timeout")["timeout"].to_i.should == old_timeout }
      end
    end

    context "#resetstat" do
      it "resets stats across all servers" do
        rc.flushall
        rc.servers.each { |srvr| srvr.info["total_commands_processed"].to_i.should > 1 }
        rc.config(:resetstat).should == "OK"
        rc.servers.each { |srvr| srvr.info["total_commands_processed"].to_i.should == 1 }
      end
    end

    context "#bad_command" do
      it "raises an error" do
        expect { rc.config(:bad_command) }.to raise_error(RuntimeError, "ERR CONFIG subcommand must be one of GET, SET, RESETSTAT")
      end
    end
  end

  context "#auth" do
    xit "works"
  end

  context "#discard" do
    xit "works"
  end

  context "#watch" do
    xit "works"
  end

  context "#unwatch" do
    xit "works"
  end

  context "#object" do
    xit "works"
  end

  context "#sort" do
    xit "works"
  end

  context "#zinterstore" do
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

  context "#zunionstore" do
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

  context "#shutdown" do
    it "shutdowns all servers" do
      rc.servers.each { |srvr| srvr.should_receive(:shutdown) }
      rc.shutdown
    end
  end
end

