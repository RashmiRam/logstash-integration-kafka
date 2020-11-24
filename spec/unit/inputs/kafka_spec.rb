# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/kafka"
require "concurrent"

class MockConsumer
  def initialize
    @wake = Concurrent::AtomicBoolean.new(false)
  end

  def subscribe(topics)
  end
  
  def poll(ms)
    if @wake.value
      raise org.apache.kafka.common.errors.WakeupException.new
    else
      10.times.map do
        org.apache.kafka.clients.consumer.ConsumerRecord.new("logstash", 0, 0, "key", "value")
      end
    end
  end

  def close
  end

  def wakeup
    @wake.make_true
  end
end

describe LogStash::Inputs::Kafka do
  let(:klass) { LogStash::Inputs::Kafka }
  let(:config) { { 'topics' => ['logstash'], 'consumer_threads' => 4 } }
  let(:instance) { klass.new(config) }

  subject { LogStash::Inputs::Kafka.new(config) }

  let(:instance_logger) { double("Logger").as_null_object }
  
  before do
    allow_any_instance_of(klass).to receive(:logger).and_return(instance_logger)
  end

  it "should register" do
    expect { subject.register }.to_not raise_error
  end

  context 'with client_rack' do
    let(:config) { super.merge('client_rack' => 'EU-R1') }

    it "sets broker rack parameter" do
      expect(org.apache.kafka.clients.consumer.KafkaConsumer).
          to receive(:new).with(hash_including('client.rack' => 'EU-R1')).
              and_return kafka_client = double('kafka-consumer')

      expect( subject.send(:create_consumer, 'sample_client-0') ).to be kafka_client
    end
  end

  context '#register' do
    let(:config) { super().merge(decorate_events_override) }
    let(:decorate_events_override) { { "decorate_events" => decorate_events } }
    before do
      instance.register
    end

    shared_examples('`decorate_events => none`') do
      context 'decorate_events_level' do
        subject(:decorate_events_level) { instance.decorate_events_level }
        it { is_expected.to be_empty }
        it { is_expected.to be_frozen }
      end
    end

    shared_examples('`decorate_events => basic`') do
      context 'decorate_events_level' do
        subject(:decorate_events_level) { instance.decorate_events_level }
        it { is_expected.to include :headers }
        it { is_expected.to include :properties }
        it { is_expected.to_not include :payload }
        it { is_expected.to be_frozen }
      end
    end

    shared_examples("deprecated `decorate_events` setting") do |deprecated_value|
      context 'the logger' do
        subject(:logger) { instance_logger }
        it 'receives a useful deprecation warning' do
          expect(logger).to have_received(:warn).with(/Deprecated value `#{Regexp.escape(deprecated_value)}`/)
        end
      end
    end

    context 'when `decorate_events` is `true`' do
      let(:decorate_events) { "true" }
      it_behaves_like '`decorate_events => basic`'
      include_examples "deprecated `decorate_events` setting", "true"
    end

    context 'when `decorate_events` is `false`' do
      let(:decorate_events) { "false" }
      it_behaves_like '`decorate_events => none`'
      include_examples "deprecated `decorate_events` setting", "false"
    end

    context 'when `decorate_events` is not provided' do
      let(:decorate_events_override) { Hash.new }
      it_behaves_like '`decorate_events => none`'
    end

    context 'when `decorate_events` is `basic`' do
      let(:decorate_events) { "basic" }
      include_examples '`decorate_events => basic`'
    end

    context 'when `decorate_events` is `none`' do
      let(:decorate_events) { "none" }
      include_examples '`decorate_events => none`'
    end

    context 'when `decorate_events` is `extended`' do
      let(:decorate_events) { "extended" }
      context 'decorate_events_level' do
        subject(:decorate_events_level) { instance.decorate_events_level }
        it { is_expected.to include :headers }
        it { is_expected.to include :properties }
        it { is_expected.to include :payload }
        it { is_expected.to be_frozen }
      end
    end
  end
end
