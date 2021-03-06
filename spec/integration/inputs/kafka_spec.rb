# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/kafka"
require "rspec/wait"
require "stud/try"
require "faraday"
require "json"

# Please run kafka_test_setup.sh prior to executing this integration test.
describe "inputs/kafka", :integration => true do
  # Group ids to make sure that the consumers get all the logs.
  let(:group_id_1) {rand(36**8).to_s(36)}
  let(:group_id_2) {rand(36**8).to_s(36)}
  let(:group_id_3) {rand(36**8).to_s(36)}
  let(:group_id_4) {rand(36**8).to_s(36)}
  let(:group_id_5) {rand(36**8).to_s(36)}
  let(:group_id_6) {rand(36**8).to_s(36)}
  let(:plain_config) do
    { 'topics' => ['logstash_integration_topic_plain'], 'codec' => 'plain', 'group_id' => group_id_1,
      'auto_offset_reset' => 'earliest' }
  end
  let(:multi_consumer_config) do
    plain_config.merge({"group_id" => group_id_4, "client_id" => "spec", "consumer_threads" => 3})
  end
  let(:snappy_config) do
    { 'topics' => ['logstash_integration_topic_snappy'], 'codec' => 'plain', 'group_id' => group_id_1,
      'auto_offset_reset' => 'earliest' }
  end
  let(:lz4_config) do
    { 'topics' => ['logstash_integration_topic_lz4'], 'codec' => 'plain', 'group_id' => group_id_1,
      'auto_offset_reset' => 'earliest' }
  end
  let(:pattern_config) do
    { 'topics_pattern' => 'logstash_integration_topic_.*', 'group_id' => group_id_2, 'codec' => 'plain',
      'auto_offset_reset' => 'earliest' }
  end
  let(:decorate_config) do
    { 'topics' => ['logstash_integration_topic_plain'], 'codec' => 'plain', 'group_id' => group_id_3,
      'auto_offset_reset' => 'earliest', 'decorate_events' => true }
  end
  let(:manual_commit_config) do
    { 'topics' => ['logstash_integration_topic_plain'], 'codec' => 'plain', 'group_id' => group_id_5,
      'auto_offset_reset' => 'earliest', 'enable_auto_commit' => 'false' }
  end
  let(:timeout_seconds) { 30 }
  let(:num_events) { 103 }

  describe "#kafka-topics" do

    it "should consume all messages from plain 3-partition topic" do
      queue = consume_messages(plain_config, timeout: timeout_seconds, event_count: num_events)
      expect(queue.length).to eq(num_events)
    end

    it "should consume all messages from snappy 3-partition topic" do
      queue = consume_messages(snappy_config, timeout: timeout_seconds, event_count: num_events)
      expect(queue.length).to eq(num_events)
    end

    it "should consume all messages from lz4 3-partition topic" do
      queue = consume_messages(lz4_config, timeout: timeout_seconds, event_count: num_events)
      expect(queue.length).to eq(num_events)
    end

    it "should consumer all messages with multiple consumers" do
      consume_messages(multi_consumer_config, timeout: timeout_seconds, event_count: num_events) do |queue, kafka_input|
        expect(queue.length).to eq(num_events)
        kafka_input.kafka_consumers.each_with_index do |consumer, i|
          expect(consumer.metrics.keys.first.tags["client-id"]).to eq("spec-#{i}")
        end
      end
    end
  end

  context "#kafka-topics-pattern" do
    it "should consume all messages from all 3 topics" do
      total_events = num_events * 3
      queue = consume_messages(pattern_config, timeout: timeout_seconds, event_count: total_events)
      expect(queue.length).to eq(total_events)
    end
  end

  context "#kafka-decorate" do
    it "should show the right topic and group name in decorated kafka section" do
      start = LogStash::Timestamp.now.time.to_i
      consume_messages(decorate_config, timeout: timeout_seconds, event_count: num_events) do |queue, _|
        expect(queue.length).to eq(num_events)
        event = queue.shift
        expect(event.get("[@metadata][kafka][topic]")).to eq("logstash_integration_topic_plain")
        expect(event.get("[@metadata][kafka][consumer_group]")).to eq(group_id_3)
        expect(event.get("[@metadata][kafka][timestamp]")).to be >= start
      end
    end
  end

  context "#kafka-offset-commit" do
    it "should manually commit offsets" do
      queue = consume_messages(manual_commit_config, timeout: timeout_seconds, event_count: num_events)
      expect(queue.length).to eq(num_events)
    end
  end

  context 'setting partition_assignment_strategy' do
    let(:test_topic) { 'logstash_integration_partitioner_topic' }
    let(:consumer_config) do
      plain_config.merge(
          "topics" => [test_topic],
          'group_id' => group_id_6,
          "client_id" => "partition_assignment_strategy-spec",
          "consumer_threads" => 2,
          "partition_assignment_strategy" => partition_assignment_strategy
      )
    end
    let(:partition_assignment_strategy) { nil }

    # NOTE: just verify setting works, as its a bit cumbersome to do in a unit spec
    [ 'range', 'round_robin', 'sticky', 'org.apache.kafka.clients.consumer.CooperativeStickyAssignor' ].each do |partition_assignment_strategy|
      describe partition_assignment_strategy do
        let(:partition_assignment_strategy) { partition_assignment_strategy }
        it 'consumes data' do
          consume_messages(consumer_config, timeout: false, event_count: 0)
        end
      end
    end
  end
end

private

def consume_messages(config, queue: Queue.new, timeout:, event_count:)
  kafka_input = LogStash::Inputs::Kafka.new(config)
  t = Thread.new { kafka_input.run(queue) }
  begin
    t.run
    wait(timeout).for { queue.length }.to eq(event_count) unless timeout.eql?(false)
    block_given? ? yield(queue, kafka_input) : queue
  ensure
    t.kill
    t.join(30_000)
  end
end


describe "schema registry connection options" do
  context "remote endpoint validation" do
    it "should fail if not reachable" do
      config = {'schema_registry_url' => 'http://localnothost:8081'}
      kafka_input = LogStash::Inputs::Kafka.new(config)
      expect { kafka_input.register }.to raise_error LogStash::ConfigurationError, /Schema registry service doesn't respond.*/
    end

    it "should fail if any topic is not matched by a subject on the schema registry" do
      config = {
        'schema_registry_url' => 'http://localhost:8081',
        'topics' => ['temperature_stream']
      }

      kafka_input = LogStash::Inputs::Kafka.new(config)
      expect { kafka_input.register }.to raise_error LogStash::ConfigurationError, /The schema registry does not contain definitions for required topic subjects: \["temperature_stream-value"\]/
    end

    context "register with subject present" do
      SUBJECT_NAME = "temperature_stream-value"

      before(:each) do
        response = save_avro_schema_to_schema_registry(File.join(Dir.pwd, "spec", "unit", "inputs", "avro_schema_fixture_payment.asvc"), SUBJECT_NAME)
        expect( response.status ).to be(200)
      end

      after(:each) do
        schema_registry_client = Faraday.new('http://localhost:8081')
        delete_remote_schema(schema_registry_client, SUBJECT_NAME)
      end

      it "should correctly complete registration phase" do
        config = {
          'schema_registry_url' => 'http://localhost:8081',
          'topics' => ['temperature_stream']
        }
        kafka_input = LogStash::Inputs::Kafka.new(config)
        kafka_input.register
      end
    end
  end
end

def save_avro_schema_to_schema_registry(schema_file, subject_name)
  raw_schema = File.readlines(schema_file).map(&:chomp).join
  raw_schema_quoted = raw_schema.gsub('"', '\"')
  response = Faraday.post("http://localhost:8081/subjects/#{subject_name}/versions",
          '{"schema": "' + raw_schema_quoted + '"}',
          "Content-Type" => "application/vnd.schemaregistry.v1+json")
  response
end

def delete_remote_schema(schema_registry_client, subject_name)
  expect(schema_registry_client.delete("/subjects/#{subject_name}").status ).to be(200)
  expect(schema_registry_client.delete("/subjects/#{subject_name}?permanent=true").status ).to be(200)
end

# AdminClientConfig = org.alpache.kafka.clients.admin.AdminClientConfig

describe "Schema registry API", :integration => true do

  let(:schema_registry) { Faraday.new('http://localhost:8081') }

  context 'listing subject on clean instance' do
    it "should return an empty set" do
      subjects = JSON.parse schema_registry.get('/subjects').body
      expect( subjects ).to be_empty
    end
  end

  context 'send a schema definition' do
    it "save the definition" do
      response = save_avro_schema_to_schema_registry(File.join(Dir.pwd, "spec", "unit", "inputs", "avro_schema_fixture_payment.asvc"), "schema_test_1")
      expect( response.status ).to be(200)
      delete_remote_schema(schema_registry, "schema_test_1")
    end

    it "delete the schema just added" do
      response = save_avro_schema_to_schema_registry(File.join(Dir.pwd, "spec", "unit", "inputs", "avro_schema_fixture_payment.asvc"), "schema_test_1")
      expect( response.status ).to be(200)

      expect( schema_registry.delete('/subjects/schema_test_1?permanent=false').status ).to be(200)
      sleep(1)
      subjects = JSON.parse schema_registry.get('/subjects').body
      expect( subjects ).to be_empty
    end
  end

  context 'use the schema to serialize' do
    after(:each) do
      expect( schema_registry.delete('/subjects/topic_avro-value').status ).to be(200)
      sleep 1
      expect( schema_registry.delete('/subjects/topic_avro-value?permanent=true').status ).to be(200)

      Stud.try(3.times, [StandardError, RSpec::Expectations::ExpectationNotMetError]) do
        wait(10).for do
          subjects = JSON.parse schema_registry.get('/subjects').body
          subjects.empty?
        end.to be_truthy
      end
    end

    let(:group_id_1) {rand(36**8).to_s(36)}

    let(:avro_topic_name) { "topic_avro" }

    let(:plain_config) do
      { 'schema_registry_url' => 'http://localhost:8081',
        'topics' => [avro_topic_name],
        'codec' => 'plain',
        'group_id' => group_id_1,
        'auto_offset_reset' => 'earliest' }
    end

    def delete_topic_if_exists(topic_name)
      props = java.util.Properties.new
      props.put(Java::org.apache.kafka.clients.admin.AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

      admin_client = org.apache.kafka.clients.admin.AdminClient.create(props)
      topics_list = admin_client.listTopics().names().get()
      if topics_list.contains(topic_name)
        result = admin_client.deleteTopics([topic_name])
        result.values.get(topic_name).get()
      end
    end

    def write_some_data_to(topic_name)
      props = java.util.Properties.new
      config = org.apache.kafka.clients.producer.ProducerConfig

      serdes_config = Java::io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
      props.put(serdes_config::SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")

      props.put(config::BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(config::KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.java_class)
      props.put(config::VALUE_SERIALIZER_CLASS_CONFIG, Java::io.confluent.kafka.serializers.KafkaAvroSerializer.java_class)

      parser = org.apache.avro.Schema::Parser.new()
      user_schema = '''{"type":"record",
                        "name":"myrecord",
                        "fields":[
                            {"name":"str_field", "type": "string"},
                            {"name":"map_field", "type": {"type": "map", "values": "string"}}
                        ]}'''
      schema = parser.parse(user_schema)
      avro_record = org.apache.avro.generic.GenericData::Record.new(schema)
      avro_record.put("str_field", "value1")
      avro_record.put("map_field", {"inner_field" => "inner value"})

      producer = org.apache.kafka.clients.producer.KafkaProducer.new(props)
      record = org.apache.kafka.clients.producer.ProducerRecord.new(topic_name, "avro_key", avro_record)
      producer.send(record)
    end

    it "stored a new schema using Avro Kafka serdes" do
      delete_topic_if_exists avro_topic_name
      write_some_data_to avro_topic_name

      subjects = JSON.parse schema_registry.get('/subjects').body
      expect( subjects ).to contain_exactly("topic_avro-value")

      num_events = 1
      queue = consume_messages(plain_config, timeout: 30, event_count: num_events)
      expect(queue.length).to eq(num_events)
      elem = queue.pop
      expect( elem.to_hash).not_to include("message")
      expect( elem.get("str_field") ).to eq("value1")
      expect( elem.get("map_field")["inner_field"] ).to eq("inner value")
    end
  end
end