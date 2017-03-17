require_relative './helper'
require 'yaml'

Elasticsearch = Embulk::Input::Elasticsearch

module Embulk
  class Input::Elasticsearch
    class TestTransaction < Test::Unit::TestCase
      def control
        Proc.new {|task| task_reports = [] }
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
