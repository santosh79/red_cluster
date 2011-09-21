require 'rubygems'
require 'mocha'
require 'fakeredis'
require File.join(__FILE__,'../red_cluster')

describe RedCluster::Server do
  it "gets initialized with the host and port as keys to hashes" do
    s = RedCluster::Server.new :host => "127.0.0.1", :port => "63"
  end
  it "two servers are equal if they share the same host and port" do
    s1 = RedCluster::Server.new :host => "127.0.0.1", :port => "63"
    s2 = RedCluster::Server.new :host => "127.0.0.1", :port => "63"
    s1.should == s2
  end
end

describe RedCluster do
  let(:rc) { RedCluster.new [{:host => "127.0.0.1", :port => "6379"}, {:host => "127.0.0.1", :port => "7379"}] }

  it "gets initialized with an array of hashes containing server info" do
    servers = [{:host => "127.0.0.1", :port => "6379"}, {:host => "127.0.0.1", :port => "7379"}]
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

