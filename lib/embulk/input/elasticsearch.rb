require_relative 'elasticsearch/connection'
require_relative 'elasticsearch/input_thread'
require_relative 'elasticsearch/converter'

module Embulk
  module Input

    class Elasticsearch < InputPlugin
      Plugin.register_input("elasticsearch", self)
      ADD_QUERY_TO_RECORD_KEY = 'query'

      def self.transaction(config, &control)
        task = {
          "nodes" => config.param("nodes", :array),
          "request_timeout" => config.param("request_timeout", :integer, default: 60),
          "index" => config.param("index", :string),
          "reload_connections" => config.param("reload_connections", :bool, default: true),
          "reload_on_failure" => config.param("reload_on_failure", :bool, default: false),
          "index_type" => config.param("index_type", :string, default: nil),
          "retry_on_failure" => config.param("retry_on_failure", :integer, default: 5),
          "per_size" => config.param("per_size", :integer, default: 1000),
          "limit_size" => config.param("limit_size", :integer, default: nil),
          "fields" => config.param("fields", :array, default: nil),
          "queries" => config.param("queries", :array),
          "sort" => config.param("sort", :hash, default: nil),
          "add_query_to_record" => config.param("add_query_to_record", :bool, default: false),
          "scroll" => config.param("scroll", :string, default: '1m')
        }
        # TODO: want max_threads
        define_num_threads = config.param("num_threads", :integer, default: 1)
        task['slice_queries'] = InputThread.get_slice_from_num_threads(task['queries'], define_num_threads)

        columns = []
        task['fields'].each_with_index{ |field, i|
          columns << Column.new(i, field['name'], field['type'].to_sym)
        }
        if task['add_query_to_record']
          columns << Column.new(task['fields'].size, ADD_QUERY_TO_RECORD_KEY, :string)
        end

        resume(task, columns, task['slice_queries'].size, &control)
      end

      def self.resume(task, columns, count, &control)
        task_reports = yield(task, columns, count)

        next_config_diff = {}
        return next_config_diff
      end

      def init
        @queries = task['slice_queries'][@index]
        Embulk.logger.info("this thread queries => #{@queries}")
        @add_query_to_record = task['add_query_to_record']
        @connection = Connection.new(task)
      end

      def run
        @queries.each do |query|
          @connection.search_with_query(query) { |result|
            if @add_query_to_record
              result << query
            end
            page_builder.add(result)
          }
        end
        page_builder.finish

        task_report = {}
        return task_report
      end
    end
  end
end
