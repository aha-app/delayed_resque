require 'spec_helper'

RSpec.describe DelayedResque::PerformableMethod do
  include PerformJob

  class DummyObject
    def self.do_something(*args)
    end
  end

  around do |ex|
    travel_to(Time.current) { ex.run }
  end

  let(:redis) { Resque.redis }
  let(:object) { 'CLASS:DummyObject' }
  let(:method) { :do_something }
  let(:method_args) { [123] }
  let(:base_job_options) do
    {
      'obj' => object,
      'method' => method,
      'args' => method_args
    }
  end
  let(:encoded_job_key) { Resque.encode(base_job_options) }
  let(:additional_job_options) { {} }
  let(:options) { base_job_options.merge(additional_job_options) }
  let(:performable_class) { DummyObject }
  let(:performable) do
    described_class.new(performable_class, method, additional_job_options, method_args)
  end
  let(:uuids) { Array.new(10) { SecureRandom.uuid } }

  before do
    uuids
    SecureRandom.stub(:uuid).and_return(*uuids)
  end

  describe '#queue' do
    subject(:queue) { performable.queue }

    context 'when there is no queue defined' do
      let(:performable_class) do
        Class.new do
          def self.do_something; end
        end
      end

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
      expect(store).to include('obj' => object)
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
        expect(store[described_class::UNIQUE_JOB_ID]).to eq(uuids.first)
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

  describe '#unique_job_id' do
    subject(:unique_job_id) { performable.unique_job_id }

    context 'when options do not include unique' do
      it 'does not set a unique job id' do
        expect(unique_job_id).to be_nil
      end
    end

    context 'when options include unique: true' do
      let(:additional_job_options) { { unique: true } }

      it 'generates a unique job id' do
        expect(unique_job_id).to eq(uuids.first)
      end

      it 'maintains a stable unique job id for this instance' do
        expect(unique_job_id).to eq(performable.unique_job_id)
      end
    end
  end

  describe '#track_unique_job' do
    subject(:track_unique_job) { performable.track_unique_job }

    context 'when options do not include unique' do
      it 'is a no-op' do
        expect { track_unique_job }.to_not(change { redis.hgetall(described_class::UNIQUE_JOBS_NAME) })
      end
    end

    context 'when options include unique: true' do
      let(:additional_job_options) { { unique: true } }

      context 'when there is no existing entry for this job' do
        it 'saves the uuid in the unique jobs hash' do
          track_unique_job
          expect(redis.hget(described_class::UNIQUE_JOBS_NAME, encoded_job_key)).to eq(uuids.first)
        end
      end

      context 'when there is already an entry for this job' do
        before do
          redis.hset(
            described_class::UNIQUE_JOBS_NAME,
            encoded_job_key,
            SecureRandom.uuid
          )
        end

        it 'overwrites the uuid in the unique jobs hash' do
          expect { track_unique_job }.to(change { redis.hget(described_class::UNIQUE_JOBS_NAME, encoded_job_key) }.from(uuids.first).to(uuids.second))
        end
      end
    end
  end

  describe '.untrack_unique_job' do
    subject(:untrack_unique_job) { described_class.untrack_unique_job(options) }

    let(:additional_job_options) { { described_class::UNIQUE_JOB_ID => SecureRandom.uuid } }


    context 'when there is no matching unique job being tracked' do
      it 'no-ops' do
        expect { untrack_unique_job }.to_not(change { redis.hgetall(described_class::UNIQUE_JOBS_NAME) })
      end
    end

    context 'when a unique job is being tracked' do
      before do
        redis.hset(
          described_class::UNIQUE_JOBS_NAME,
          encoded_job_key,
          uuids.first
        )
      end

      context 'when there is a unique job id' do
        it 'removes the matching hash entry' do
          expect { untrack_unique_job }.to(
            change { redis.hget(described_class::UNIQUE_JOBS_NAME, encoded_job_key) }
              .from(uuids.first).to(nil)
          )
        end
      end

      context 'when there is no unique job id' do
        let(:additional_job_options) { {} }

        it 'no-ops' do
          expect { untrack_unique_job }.to_not(change { redis.hgetall(described_class::UNIQUE_JOBS_NAME) })
        end
      end
    end
  end

  describe '.last_unique_job_id' do
    subject(:last_unique_job_id) { described_class.last_unique_job_id(options) }

    let(:additional_job_options) { { described_class::UNIQUE_JOB_ID => uuids.first } }
    let(:tracked_uuid) { uuids.second }

    context 'when there is not a tracked job id' do
      it 'returns nothing' do
        expect(last_unique_job_id).to be_nil
      end
    end

    context 'when there is a tracked job id for this job' do
      before do
        redis.hset(
          described_class::UNIQUE_JOBS_NAME,
          encoded_job_key,
          tracked_uuid
        )
      end

      it 'returns the tracked job id' do
        expect(last_unique_job_id).to eq(tracked_uuid)
      end
    end
  end

  describe '.perform' do
    subject(:perform) { perform_job(described_class, options) }

    context 'when job is not unique' do
      let(:additional_job_options) { { 't' => Time.now.to_f } }

      it 'executes the method' do
        DummyObject.should_receive(:do_something).with(*method_args).once
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
            DummyObject.should_receive(:do_something).with(*method_args).once
            perform
          end

          it 'untracks the job' do
            expect { perform }.to(change { redis.hexists(described_class::UNIQUE_JOBS_NAME, encoded_job_key) }.from(true).to(false))
          end
        end

        context 'when this job is not the last unique job' do
          let(:tracked_uuid) { other_uuid }

          it 'does not execute the method' do
            DummyObject.should_not_receive(:do_something)
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
          DummyObject.should_not_receive(:do_something)
          perform
        end
      end
    end
  end
end
