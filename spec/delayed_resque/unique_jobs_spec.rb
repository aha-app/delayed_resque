require 'spec_helper'

RSpec.describe DelayedResque::UniqueJobs do
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
  let(:encoded_job_key) { Digest::SHA256.hexdigest(Resque.encode(base_job_options)) }
  let(:additional_job_options) { {} }
  let(:options) { base_job_options.merge(additional_job_options) }
  let(:uuids) { Array.new(2) { SecureRandom.uuid } }

  before do
    uuids
    allow(SecureRandom).to receive(:uuid).and_return(*uuids)
  end

  let(:performable_class) do
    Class.new do
      include DelayedResque::UniqueJobs

      def initialize(options)
        @options = options
      end

      def queue
        @options[:queue] || :default
      end
    end
  end

  describe '#unique_job_id' do
    subject(:unique_job_id) { performable.unique_job_id }

    let(:performable) { performable_class.new(job_options) }

    context 'when options do not include unique' do
      let(:job_options) { { at: 2.weeks.from_now } }

      it 'does not set a unique job id' do
        expect(unique_job_id).to be_nil
      end
    end

    context 'when options include unique: true' do
      let(:job_options) { { unique: true } }

      it 'generates a unique job id' do
        expect(unique_job_id).to eq("default_#{uuids.first}")
      end

      it 'maintains a stable unique job id for this instance' do
        expect(unique_job_id).to eq(performable.unique_job_id)
      end

      context 'with non-default queue' do
        let(:job_options) { { unique: true, queue: :custom } }

        it 'generates a unique job id' do
          expect(unique_job_id).to eq("custom_#{uuids.first}")
        end
      end
    end
  end

  describe '.track_unique_job' do
    subject(:track_unique_job) { performable_class.track_unique_job(options) }

    let(:additional_job_options) { { performable_class::UNIQUE_JOB_ID => uuids.first } }

    context 'when there is no existing entry for this job' do
      it 'saves the uuid in the unique jobs hash' do
        track_unique_job
        expect(redis.hget(performable_class::UNIQUE_JOBS_NAME, encoded_job_key)).to eq(uuids.first)
      end
    end

    context 'when there is already an entry for this job' do
      before do
        redis.hset(
          performable_class::UNIQUE_JOBS_NAME,
          encoded_job_key,
          uuids.second
        )
      end

      it 'overwrites the uuid in the unique jobs hash' do
        expect { track_unique_job }.to(change { redis.hget(performable_class::UNIQUE_JOBS_NAME, encoded_job_key) }.from(uuids.second).to(uuids.first))
      end
    end
  end

  describe '.untrack_unique_job' do
    subject(:untrack_unique_job) { performable_class.untrack_unique_job(options) }

    let(:additional_job_options) { { performable_class::UNIQUE_JOB_ID => uuids.first } }

    before do
      redis.hset(
        performable_class::UNIQUE_JOBS_NAME,
        encoded_job_key,
        uuids.first
      )
    end

    it 'removes the matching hash entry' do
      expect { untrack_unique_job }.to(
        change { redis.hget(performable_class::UNIQUE_JOBS_NAME, encoded_job_key) }
          .from(uuids.first).to(nil)
      )
    end
  end

  describe '.last_unique_job_id' do
    subject(:last_unique_job_id) { performable_class.last_unique_job_id(options) }

    let(:additional_job_options) { { performable_class::UNIQUE_JOB_ID => uuids.first } }
    let(:tracked_uuid) { uuids.second }

    context 'when there is not a tracked job id' do
      it 'returns nothing' do
        expect(last_unique_job_id).to be_nil
      end
    end

    context 'when there is a tracked job id for this job' do
      before do
        redis.hset(
          performable_class::UNIQUE_JOBS_NAME,
          encoded_job_key,
          tracked_uuid
        )
      end

      it 'returns the tracked job id' do
        expect(last_unique_job_id).to eq(tracked_uuid)
      end
    end
  end

  describe '.unique_job?' do
    subject(:unique_job?) do
      performable_class.unique_job?(options)
    end

    context 'when there is a unique job id' do
      let(:additional_job_options) { { performable_class::UNIQUE_JOB_ID => uuids.first } }

      it "is true" do
        expect(unique_job?).to eq(true)
      end
    end

    context 'when there is not unique job id' do
      it "is false" do
        expect(unique_job?).to eq(false)
      end
    end
  end

  describe '.last_unique_job?' do
    subject(:last_unique_job?) do
      performable_class.last_unique_job?(options)
    end

    before do
      redis.hset(
        performable_class::UNIQUE_JOBS_NAME,
        encoded_job_key,
        tracked_uuid
      )
    end

    context 'when there is a unique job id' do
      let(:additional_job_options) { { performable_class::UNIQUE_JOB_ID => uuids.first } }

      context 'when the job id is last' do
        let(:tracked_uuid) { uuids.first }

        it 'is true' do
          expect(last_unique_job?).to eq(true)
        end
      end

      context 'when a different job id is last' do
        let(:tracked_uuid) { uuids.second }

        it 'is false' do
          expect(last_unique_job?).to eq(false)
        end
      end
    end

    context 'when there is not unique job id' do
      let(:tracked_uuid) { uuids.first }

      it 'is false' do
        expect(last_unique_job?).to eq(false)
      end
    end
  end

  describe '.unique_job_key' do
    def unique_job_key_with(additional_options = {})
      performable_class.unique_job_key(base_job_options.merge(additional_options))
    end

    it 'is the same when job options are the same' do
      expect(unique_job_key_with).to eq(unique_job_key_with)
    end

    it 'is the same when job uuids are different' do
      key_with_uuid1 = unique_job_key_with(performable_class::UNIQUE_JOB_ID => uuids.first)
      key_with_uuid2 = unique_job_key_with(performable_class::UNIQUE_JOB_ID => uuids.second)
      expect(key_with_uuid1).to eq(key_with_uuid2)
    end

    it 'is different when performable class is different' do
      key_dummy_object = unique_job_key_with(obj: 'CLASS:DummyObject')
      key_dummy_model = unique_job_key_with(klass: 'AR:DummyModel:12345')
      expect(key_dummy_object).to_not eq(key_dummy_model)
    end

    it 'is different when method name is different' do
      expect(unique_job_key_with(method: :do_something)).to_not eq(unique_job_key_with(method: :do_something_else))
    end

    it 'is different when args are different' do
      expect(unique_job_key_with(args: [:foo])).to_not eq(unique_job_key_with(args: [:bar]))
    end

    it 'is different when params are different' do
      key_param1 = unique_job_key_with(additional_options: { params: { foo: :bar } })
      key_param2 = unique_job_key_with(additional_options: { params: { foo: :baz } })
      expect(key_param1).to_not eq(key_param2)
    end

    it 'is different when tracked are different' do
      key_tracked1 = unique_job_key_with(additional_options: { tracked: 'abc123' })
      key_tracked2 = unique_job_key_with(additional_options: { tracked: 'xyz789' })
      expect(key_tracked1).to_not eq(key_tracked2)
    end
  end

  describe '.generate_unique_job_id' do
    def generate_unique_job_id
      performable_class.generate_unique_job_id(queue: queue)
    end

    context 'with default queue' do
      let(:queue) {}

      it 'generates a unique job id' do
        expect(generate_unique_job_id).to eq("default_#{uuids.first}")
      end

      it 'generates a new job id each time it is called' do
        generate_unique_job_id
        expect(generate_unique_job_id).to eq("default_#{uuids.second}")
      end
    end

    context 'with non-default queue' do
      let(:queue) { :custom }

      it 'includes the queue name in the job id' do
        expect(generate_unique_job_id).to eq("custom_#{uuids.first}")
      end
    end
  end
end
