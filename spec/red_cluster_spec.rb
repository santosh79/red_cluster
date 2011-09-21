require 'rubygems'
require 'fakeredis'
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
      rc.servers.select { |server| server.cnx.get("foo") != nil }.size.should == 1
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

  context "#flushall" do
    it "flushes keys from all across the cluster" do
      (1..10_000).to_a.each { |num| rc.set("number|#{num}", "hello") }
      first_server_rand_key = rc.servers.first.cnx.randomkey
      first_server_rand_key.should be
      second_server_rand_key = rc.servers.last.cnx.randomkey
      second_server_rand_key.should be
      rc.flushall.should == "OK"
      rc.randomkey.should_not be
    end
  end

  context "#keys" do
    it 'scans across the cluster' do
      (1..1000).to_a.each { |num| rc.set("number|#{num}", "hello") }
      first_servers_keys = rc.servers.first.cnx.keys("*")
      first_servers_keys.size.should > 0
      second_servers_keys = rc.servers.last.cnx.keys("*")
      second_servers_keys.size.should > 0
      first_servers_keys.map(&:to_s).sort.should_not == second_servers_keys.map(&:to_s).sort
      all_keys = rc.keys("*")
      all_keys.map(&:to_s).sort.should == (first_servers_keys + second_servers_keys).map(&:to_s).sort
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
      rc.sinterstore("result_set", "unknown_set", "set_two").should == 0
      rc.smembers("result_set").should == []
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
      rc.sunionstore("result_set", "unknown_set", "set_two").should == 0
      rc.smembers("result_set").should == []
    end
  end

  context "#rename" do
    it "raises an error if the key did not exist" do
      expect { rc.rename("unknown_key", "key") }.should raise_error
    end
    it "raises an error if the keys are the same" do
      rc.set "foo", "bar"
      expect { rc.rename("foo", "foo") }.should raise_error
    end
    it "does a rename" do
      rc.set "foo", "bar"
      rc.rename("foo", "foo_new").should == "OK"
      rc.exists("foo").should_not be
      rc.get("foo_new").should == "bar"
    end
  end
end

describe RedCluster::Server do
  it "gets initialized with the host and port as keys to hashes" do
    s = RedCluster::Server.new :host => "127.0.0.1", :port => "6379"
  end
  it "two servers are equal if they share the same host and port" do
    s1 = RedCluster::Server.new :host => "127.0.0.1", :port => "6379"
    s2 = RedCluster::Server.new :host => "127.0.0.1", :port => "6379"
    s1.should == s2
  end
end

