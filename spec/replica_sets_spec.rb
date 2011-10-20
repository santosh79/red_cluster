require 'spec_helper'
require 'replica_set'

describe RedCluster::ReplicaSet do
  context "#initializtion" do
    it "gets initialized with one master & one or more slaves" do
      master = {:host => "localhost", :port => 6379}
      slaves = [{:host => "localhost", :port => 7379},
                {:host => "localhost", :port => 8379}]
      RedCluster::ReplicaSet.new :master => master, :slaves => slaves
    end
  end

  context "#other characteristics" do
    before(:all) do
      master = {:host => "localhost", :port => 6379}
      slaves = [{:host => "localhost", :port => 7379},
        {:host => "localhost", :port => 8379}]
      @rs = RedCluster::ReplicaSet.new :master => master, :slaves => slaves
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
      xit "get forwarded to the master"
    end

    context "#blocking operations" do
      it "raise an error" do
        expect { rs.blpop("some_list", 0) }.to raise_error
      end
    end

    context "#pub sub operations" do
      it "raise an error" do
        expect { rs.blpop("publish", 0) }.to raise_error
      end
    end

    context "master dying" do
      before(:each) do
        master.stubs(:ping).raises RuntimeError
      end
      context "when there are more than one slave" do
        xit "one of them gets promoted to the new master"
        xit "the other's become slaves of the new master"
      end
      context "when there is just one slave" do
        xit "becomes the new master"
      end
      context "when there are no slaves" do
        xit "a RedCluster::NoMaster exception get's thrown"
      end
    end
  end
end

