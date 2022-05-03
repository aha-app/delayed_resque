require 'spec_helper'
require 'resque_spec/scheduler'

describe DelayedResque do
  class DummyObject
    include DelayedResque::MessageSending
    @queue = "default"

    def self.first_method(param)
    end
  end

  context "class methods can be delayed" do
    it "can delay method" do
      travel_to Time.current do
        DummyObject.delay.first_method(123)
        expect(DelayedResque::PerformableMethod).to have_queued({"obj"=>"CLASS:DummyObject", "method"=>:first_method, "args"=>[123], "t" => Time.current.to_f}).in(:default)
      end
    end

    it "delayed method is called" do
      DummyObject.stub(:second_method).with(123, 456)
      with_resque do
        DummyObject.delay.second_method(123, 456)
      end
    end

    it "can't delay missing method" do
      expect {
        DummyObject.delay.non_existent_method
      }.to raise_error(NoMethodError)
    end

    it "can pass additional params" do
      travel_to Time.current do
        DummyObject.delay(:params => {"k" => "v"}).first_method(123)
        expect(DelayedResque::PerformableMethod).to have_queued({"obj"=>"CLASS:DummyObject", "method"=>:first_method, "args"=>[123], "k" => "v", "t" => Time.current.to_f}).in(:default)
      end
    end

  end

  context "active record methods can be delayed" do

    ActiveRecord::Base.connection.execute("DROP TABLE IF EXISTS 'dummy_models'")
    ActiveRecord::Base.connection.create_table(:dummy_models) do |t|
      t.integer :value
    end

    class DummyModel < ActiveRecord::Base
      def update_value(new_value1, new_value2)
        self.value = new_value1 + new_value2
        save!
      end

      def copy_value(record)
        self.value = record.value
        save!
      end
    end

    it "can delay method" do
      record = DummyModel.create(:value => 1)
      with_resque do
        record.delay.update_value(3, 7)
      end
      expect(record.reload.value).to eq(10)
    end

    it "AR model can be parameter to delay" do
      record1 = DummyModel.create(:value => 1)
      record2 = DummyModel.create(:value => 3)
      with_resque do
        record1.delay.copy_value(record2)
      end
      expect(record1.reload.value).to eq(3)
    end

  end

  context "tasks can be tracked" do
    it "adds tracking params tasks" do
      travel_to Time.current do
        DummyObject.delay(tracked: "4").first_method(123)
        expect(DelayedResque::PerformableMethod).to have_queued({"obj"=>"CLASS:DummyObject", "method"=>:first_method, "args"=>[123], "tracked_task_key"=> "4", "t" => Time.current.to_f}).in(:default)
      end
    end

    it "adds tracking key to redis" do
      DummyObject.delay(tracked: "4").first_method(123)
      expect(DelayedResque::DelayProxy.tracked_task?("4")).to eq(true)
    end

  end

  context "methods can be delayed for an interval" do
    it "can delay method" do
      travel_to Time.current do
        DummyObject.delay(:in => 5.minutes).first_method(123)
        expect(DelayedResque::PerformableMethod).to have_scheduled({"obj"=>"CLASS:DummyObject", "method"=>:first_method, "args"=>[123]}).in(5 * 60)
      end
    end

    it "can run at specific time" do
      at_time = Time.now.utc + 10.minutes
      DummyObject.delay(:at => at_time).first_method(123)
      expect(DelayedResque::PerformableMethod).to have_scheduled({"obj"=>"CLASS:DummyObject", "method"=>:first_method, "args"=>[123]}).at(at_time)
      expect(DelayedResque::PerformableMethod).to have_schedule_size_of(1)
    end
  end

  context "unique jobs" do
    around do |ex|
      # Freeze time to make comparison easy (and also to test against relying
      # on timestamps for uniqueness)
      travel_to(Time.current) do
        ex.run
      end
    end

    let(:uuids) { Array.new(5) { SecureRandom.uuid } }

    before do
      uuids
      SecureRandom.stub(:uuid).and_return(*uuids)
    end

    it 'enqueues non-scheduled unique jobs, keeping track of the last' do
      stored_args = {
        'obj' => 'CLASS:DummyObject',
        'method' => :first_method,
        'args' => [123],
        DelayedResque::PerformableMethod::UNIQUE_JOB_ID => "default_#{uuids.first}"
      }

      expect(DelayedResque::PerformableMethod.last_unique_job_id(stored_args)).to be_nil

      DummyObject.delay(unique: true).first_method(123)

      expect(DelayedResque::PerformableMethod.last_unique_job_id(stored_args)).to eq("default_#{uuids.first}")
      expect(DelayedResque::PerformableMethod).to have_queued(stored_args)
      expect(DelayedResque::PerformableMethod).to have_queue_size_of(1)

      stored_args = {
        'obj' => 'CLASS:DummyObject',
        'method' => :first_method,
        'args' => [124],
        't' => Time.now.to_f
      }

      expect(DelayedResque::PerformableMethod.last_unique_job_id(stored_args)).to be_nil

      DummyObject.delay.first_method(124)

      expect(DelayedResque::PerformableMethod.last_unique_job_id(stored_args)).to be_nil
      expect(DelayedResque::PerformableMethod).to have_queued(stored_args)
      expect(DelayedResque::PerformableMethod).to have_queue_size_of(2)

      stored_args = {
        'obj' => 'CLASS:DummyObject',
        'method' => :first_method,
        'args' => [123],
        DelayedResque::PerformableMethod::UNIQUE_JOB_ID => "default_#{uuids.second}"
      }

      expect(DelayedResque::PerformableMethod.last_unique_job_id(stored_args)).to eq("default_#{uuids.first}")

      DummyObject.delay(unique: true).first_method(123)

      expect(DelayedResque::PerformableMethod).to have_queued(stored_args)
      expect(DelayedResque::PerformableMethod).to have_queue_size_of(3)
      expect(DelayedResque::PerformableMethod.last_unique_job_id(stored_args)).to eq("default_#{uuids.second}")
    end

    it "enqueues delayed jobs, keeping track of the last" do
      at_time = Time.now.utc + 10.minutes
      DummyObject.delay(:at => at_time).first_method(123)

      stored_args = {
        'obj' => 'CLASS:DummyObject',
        'method' => :first_method,
        'args' => [123],
      }

      expect(DelayedResque::PerformableMethod).to have_scheduled(stored_args).at(at_time)
      expect(DelayedResque::PerformableMethod.last_unique_job_id(stored_args)).to be_nil

      DummyObject.delay(:at => at_time + 1).first_method(123)

      expect(DelayedResque::PerformableMethod).to have_scheduled(stored_args).at(at_time)
      expect(DelayedResque::PerformableMethod).to have_scheduled(stored_args).at(at_time + 1)
      expect(DelayedResque::PerformableMethod.last_unique_job_id(stored_args)).to be_nil

      DummyObject.delay(:at => at_time + 2, :unique => true).first_method(123)

      expect(DelayedResque::PerformableMethod).to have_scheduled(stored_args).at(at_time)
      expect(DelayedResque::PerformableMethod).to have_scheduled(stored_args).at(at_time + 1)
      args_with_job_id = stored_args.merge(DelayedResque::UniqueJobs::UNIQUE_JOB_ID => "default_#{uuids.first}")
      expect(DelayedResque::PerformableMethod).to have_scheduled(args_with_job_id).at(at_time + 2)
      expect(DelayedResque::PerformableMethod.last_unique_job_id(stored_args)).to eq("default_#{uuids.first}")
    end

    it "can overwrite preceeding delayed jobs with a non-default queue" do
      at_time = Time.now.utc + 10.minutes
      DummyObject.delay(at: at_time, unique: true, queue: :send_audit).first_method(123)

      stored_args = {
        'obj' => 'CLASS:DummyObject',
        'method' => :first_method,
        'args' => [123]
      }

      args_with_job_id = stored_args.merge(DelayedResque::UniqueJobs::UNIQUE_JOB_ID => "send_audit_#{uuids.first}")
      expect(DelayedResque::PerformableMethod).to have_scheduled(args_with_job_id).at(at_time).queue(:send_audit)
      expect(DelayedResque::PerformableMethod.last_unique_job_id(args_with_job_id)).to eq("send_audit_#{uuids.first}")

      DummyObject.delay(at: at_time + 1, unique: true, queue: :send_audit).first_method(123)

      expect(DelayedResque::PerformableMethod).to have_schedule_size_of(2).queue(:send_audit)

      args_with_job_id = stored_args.merge(DelayedResque::UniqueJobs::UNIQUE_JOB_ID => "send_audit_#{uuids.second}")
      expect(DelayedResque::PerformableMethod).to have_scheduled(args_with_job_id).at(at_time + 1).queue(:send_audit)
      expect(DelayedResque::PerformableMethod.last_unique_job_id(args_with_job_id)).to eq("send_audit_#{uuids.second}")
    end
  end

  context "throttled jobs" do
    it "will schedule a job" do
      travel_to Time.current do
        DummyObject.delay(:at => 5.seconds.from_now, :throttle => true).first_method(123)
        expect(DelayedResque::PerformableMethod).to have_scheduled("obj" => "CLASS:DummyObject", "method" => :first_method, "args" => [123])
        expect(DelayedResque::PerformableMethod).to have_schedule_size_of(1)
      end
    end

    it "will not schedule a job if one is already scheduled" do
      travel_to Time.current do
        DummyObject.delay(:at => 5.minutes.from_now, :throttle => true).first_method(123)
        expect(DelayedResque::PerformableMethod).to have_scheduled("obj" => "CLASS:DummyObject", "method" => :first_method, "args" => [123])
        expect(DelayedResque::PerformableMethod).to have_schedule_size_of(1)
      end

      travel_to 1.minute.from_now do
        DummyObject.delay(:at => 5.minutes.from_now, :throttle => true).first_method(123)
        expect(DelayedResque::PerformableMethod).to have_scheduled("obj" => "CLASS:DummyObject", "method" => :first_method, "args" => [123])
        expect(DelayedResque::PerformableMethod).to have_schedule_size_of(1)
      end
    end
  end
end
