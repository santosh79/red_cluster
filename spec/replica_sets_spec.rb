require 'spec_helper'
require 'replica_set'

describe RedCluster::ReplicaSet do
  before(:each) do
    master = {:host => "localhost", :port => 6379}
    slaves = [{:host => "localhost", :port => 7379},
      {:host => "localhost", :port => 8379}]
    @rs = RedCluster::ReplicaSet.new nil, :master => master, :slaves => slaves
  end
  let(:rs) { @rs }
  let(:master) { @rs.master }
  let(:slaves) { @rs.slaves }
  after { master.flushall }

  context "#replication" do
    it "the slaves are slaveof's master" do
      slaves.each do |slave|
        slave.info["role"].should == "slave"
        slave.info["master_host"].should == master.client.host
        slave.info["master_port"].should == master.client.port.to_s
      end
    end
  end

  context "master dying" do
    before(:each) do
      master.stubs(:set).raises Errno::ECONNREFUSED
    end
    context "when it's a read op" do
      it "things work as though nothing happened" do
        expect { rs.get("foo") }.to_not raise_error
      end
    end

    context "when there are more than one slave" do
      it "one of them gets promoted to the new master" do
        old_slaves = slaves.dup
        old_master = master
        rs.set("foo", "bar")
        old_master.should_not == rs.master
        old_slaves.should include(rs.master)
      end
      it "the other's become slaves of the new master" do
        rs.set("foo", "bar")
        new_master = rs.master
        rs.slaves.each do |slave| 
          slave.info["master_host"].should == new_master.client.host
          slave.info["master_port"].should == new_master.client.port.to_s
        end
      end
    end
    context "when there is just one slave" do
      it "becomes the new master" do
        slaves.shift while slaves.count > 1
        slaves.count.should == 1
        old_slave = slaves[0]
        rs.set('foo', 'bar')
        old_slave.should == rs.master
      end
    end
    context "when there are no slaves" do
      it "a RedCluster::NoMaster exception get's thrown" do
        slaves.shift while slaves.count > 0
        expect { rs.set('foo', 'bar') }.to raise_error(RedCluster::NoMaster, "No master in replica set")
      end
    end
  end

  context "#read operations" do
    it "get forwarded to the slaves on a round-robin basis" do
      master.expects(:get).never
      slaves[0].expects(:get).with("some_key").returns "some_val"
      slaves[1].expects(:get).with("some_key").returns "some_new_val"

      rs.get("some_key").should == "some_val"
      rs.get("some_key").should == "some_new_val"
    end
  end

  context "#write operations" do
    it "get forwarded to the master" do
      master.expects(:set)
      rs.set("foo", "bar")
    end
  end

  context "#blocking operations" do
    it "raise an error" do
      expect { rs.blpop("some_list", 0) }.to raise_error
    end
  end

  context "#slaveof operations" do
    it "raise an error" do
      expect { rs.slaveof("localhost", 6379) }.to raise_error
    end
  end

  context "#pub sub operations" do
    it "raise an error" do
      expect { rs.blpop("publish", 0) }.to raise_error
    end
  end
end

