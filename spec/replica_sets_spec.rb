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
      it "get forwarded to the slave"
    end

    context "#write operations" do
      xit "get forwarded to the master"
    end

    context "master dying" do
      context "when there are more than one slave" do
      end
      context "when there is just one slave" do
      end
      context "when there are no slaves" do
      end
    end
  end
end

