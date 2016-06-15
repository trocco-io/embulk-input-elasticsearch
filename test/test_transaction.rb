require_relative './helper'
require 'embulk/input/elasticsearch'
require 'yaml'

Elasticsearch = Embulk::Input::Elasticsearch

module Embulk
  class Input::Elasticsearch
    class TestTransaction < Test::Unit::TestCase
      def least_config
        DataSource.new({})
      end

      def control
        Proc.new {|task| task_reports = [] }
      end

      sub_test_case "get_slice_from_num_threads" do
        def test_normal
          slice = Elasticsearch.get_slice_from_num_threads((1..10).to_a, 5)
          assert_equal slice.size, 5
          assert_equal slice.first.size, 2
        end

        def test_normal_same
          slice = Elasticsearch.get_slice_from_num_threads((1..3).to_a, 3)
          assert_equal slice.size, 3
          assert_equal slice.first.size, 1
        end

        def test_num_threads_over_array_size
          slice = Elasticsearch.get_slice_from_num_threads((1..3).to_a, 10)
          assert_equal slice.size, 3
          assert_equal slice.first.size, 1
        end

        def test_rest
          slice = Elasticsearch.get_slice_from_num_threads((1..20).to_a, 8)
          assert_equal slice.size, 7
          assert_equal slice.first.size, 3
        end
      end

      sub_test_case "transaction" do
        def test_normal
          yaml = YAML.load(%(
              nodes:
                - {host: localhost, port: 9200}
              queries:
                - 'title: 製函機'
              index: crawl
              index_type: m_corporation_page
              request_timeout: 60
              per_size: 1000
              limit_size: 2000
              num_threads: 20
              fields:
                - { name: title, type: string }
            )
          )
          config = DataSource.new(yaml)
          Elasticsearch.transaction(config, &control)
        end

        def test_minimum
          yaml = YAML.load(%(
              nodes:
                - {host: localhost, port: 9200}
              queries:
                - 'title: 製函機'
              index: crawl
              fields:
                - { name: title, type: string }
            )
          )
          config = DataSource.new(yaml)
          Elasticsearch.transaction(config, &control)
        end
      end
    end
  end
end