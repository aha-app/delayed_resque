require 'spec_helper'
require 'pry'

RSpec.describe DelayedResque::PerformableMethod do
  include PerformJob

  class DummyObject
    def self.do_something(*args)
    end

    def self.do_something_else(*args)
    end
  end

  class DummyModel < ActiveRecord::Base
    def self.do_something(*args)
    end

    def do_something
    end
  end

  class DummyModelChild < DummyModel
  end

  # Test will use becomes to convert model into this type of class
  class NotARealDummyModelChild < DummyModelChild
  end

  around do |ex|
    travel_to(Time.current) { ex.run }
  end

  let(:redis) { Resque.redis }
  let(:object) { DummyObject }
  let(:method) { :do_something }
  let(:method_args) { [123] }
  let(:base_job_options) do
    {
      'obj' => object,
      'method' => method,
      'args' => method_args
    }
  end
  let(:encoded_job_key) { Digest::SHA256.hexdigest(Resque.encode(base_job_options)) }
  let(:additional_job_options) { {} }
  let(:options) { base_job_options.merge(additional_job_options) }
  let(:performable) do
    described_class.new(object, method, additional_job_options, method_args)
  end
  let(:uuids) { Array.new(10) { SecureRandom.uuid } }

  before do
    uuids
    allow(SecureRandom).to receive(:uuid).and_return(*uuids)
  end

  describe '#queue' do
    subject(:queue) { performable.queue }

    context 'when there is no queue defined' do
      it 'uses the default queue' do
        expect(queue).to eq('default')
      end
    end

    context 'when there is a queue in the job options' do
      let(:additional_job_options) { { queue: :custom_queue } }

      it 'uses the queue from the job options' do
        expect(queue).to eq(:custom_queue)
      end
    end
  end

  describe '#store' do
    subject(:store) { performable.store }

    it 'has the correct obj' do
      expect(store).to include('obj' => 'CLASS:DummyObject')
    end

    it 'has the correct method' do
      expect(store).to include('method' => method)
    end

    it 'has the correct args' do
      expect(store).to include('args' => method_args)
    end

    it 'includes the current timestamp' do
      expect(store).to include('t' => Time.now.to_f)
    end

    it 'does not include a unique job id' do
      expect(store).to_not have_key(described_class::UNIQUE_JOB_ID)
    end

    context 'when object is an AR class' do
      let(:object) { DummyModel }

      it 'has the correct obj' do
        expect(store).to include('obj' => 'CLASS:DummyModel')
      end
    end

    context 'when object is an AR child STI class' do
      let(:object) { DummyModelChild }

      it 'has the correct obj' do
        expect(store).to include('obj' => 'CLASS:DummyModelChild')
      end
    end

    context 'when object is an AR instance' do
      let(:object) { DummyModel.new(id: 123) }

      it 'has the correct obj' do
        expect(store).to include('obj' => 'AR:DummyModel:123')
      end
    end

    context 'when object is an AR child class instance' do
      let(:object) { DummyModelChild.new(id: 456) }

      it 'has the correct obj' do
        expect(store).to include('obj' => 'AR:DummyModel:456')
      end
    end

    context 'when object is an AR instance created via becomes' do
      let(:object) { DummyModelChild.new(id: 789).becomes(NotARealDummyModelChild) }

      it 'has the correct obj' do
        expect(store).to include('obj' => 'AR:DummyModel:789')
      end
    end

    context 'when job options include params' do
      let(:additional_job_options) do
        { params: { a: 1, b: 2 } }
      end

      it 'includes params' do
        expect(store).to include(a: 1, b: 2)
      end
    end

    context 'when job options include unique' do
      let(:additional_job_options) { { unique: true } }

      it 'does not include the timestamp' do
        expect(store).to_not have_key('t')
      end

      it 'generates a unique job id' do
        expect(store[described_class::UNIQUE_JOB_ID]).to eq("default_#{uuids.first}")
      end

      it 'maintains a stable id for this job instance' do
        expect(performable.store[described_class::UNIQUE_JOB_ID])
          .to eq(performable.store[described_class::UNIQUE_JOB_ID])
      end
    end

    context 'when job options include thottle' do
      let(:additional_job_options) { { throttle: true } }

      it 'does not include the timestamp' do
        expect(store).to_not have_key('t')
      end
    end

    context 'when job options include at' do
      let(:additional_job_options) { { at: 10.minutes.from_now } }

      it 'does not include the timestamp' do
        expect(store).to_not have_key('t')
      end
    end

    context 'when job options include in' do
      let(:additional_job_options) { { in: 1.minute } }

      it 'does not include the timestamp' do
        expect(store).to_not have_key('t')
      end
    end
  end

  describe '.perform' do
    subject(:perform) { perform_job(described_class, options) }

    context 'when job is not unique' do
      let(:additional_job_options) { { 't' => Time.now.to_f } }

      it 'executes the method' do
        allow(DummyObject).to receive(:do_something).with(*method_args).once
        perform
      end
    end

    context 'when job has unique identifier' do
      let(:additional_job_options) { { described_class::UNIQUE_JOB_ID => uuid } }

      let(:uuid) { SecureRandom.uuid }
      let(:other_uuid) { SecureRandom.uuid }

      context 'when there is a unique job id being tracked' do
        before do
          redis.hset(
            described_class::UNIQUE_JOBS_NAME,
            encoded_job_key,
            tracked_uuid
          )
        end

        context 'when this job is the last unique job' do
          let(:tracked_uuid) { uuid }

          it 'executes the method' do
            allow(DummyObject).to receive(:do_something).with(*method_args).once
            perform
          end

          it 'untracks the job' do
            expect { perform }.to(change { redis.hexists(described_class::UNIQUE_JOBS_NAME, encoded_job_key) }.from(true).to(false))
          end
        end

        context 'when this job is not the last unique job' do
          let(:tracked_uuid) { other_uuid }

          it 'does not execute the method' do
            expect(DummyObject).to_not receive(:do_something)
            perform
          end

          it 'does not untrack the unique job' do
            expect { perform }.to_not(change { redis.hexists(described_class::UNIQUE_JOBS_NAME, encoded_job_key) }.from(true))
          end
        end
      end

      context 'when there is not a unique job id being tracked' do
        # We should never end up here in real life, but for the sake of completeness...
        # we can assume that *somehow* the unique job that was being tracked was already
        # processed and therefore this should be a no-op
        it 'does not execute the method' do
          expect(DummyObject).to_not receive(:do_something)
          perform
        end
      end
    end
  end
end
