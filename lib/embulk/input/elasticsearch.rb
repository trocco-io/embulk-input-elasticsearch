require 'excon'
require 'elasticsearch'
require_relative 'elasticsearch/connection'
require_relative 'elasticsearch/input_thread'

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
          "add_query_to_record" => config.param("add_query_to_record", :bool, default: false)
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
        @client = Connection.create_client(task)
        @index_name = task['index']
        @index_type = task['index_type']
        @per_size = task['per_size']
        @limit_size = task['limit_size']
        @fields = task['fields']
        @sort = task['sort']
        @add_query_to_record = task['add_query_to_record']
      end

      def run
        search(@index_type, @per_size, @routing, @fields, @sort)
        page_builder.finish

        task_report = {}
        return task_report
      end

      private

      def search(type, size, routing, fields, sort)
        @queries.each do |query|
          search_with_query(query, type, size, routing, fields, sort)
        end
      end

      def search_with_query(query, type, size, routing, fields, sort)
        search_option = get_search_option(type, query, size, fields, sort)
        Embulk.logger.info("#{search_option}")
        r = @client.search(search_option)
        i = 0
        get_sources(r, fields).each do |result|
          result_proc(result, query)
          return if @limit_size == (i += 1)
        end

        while r = @client.scroll(scroll_id: r['_scroll_id'], scroll: '1m') and (not r['hits']['hits'].empty?) do
          get_sources(r, fields).each do |result|
            result_proc(result, query)
            return if @limit_size == (i += 1)
          end
        end
      end

      def result_proc(result, query)
        if @add_query_to_record
          result << query
        end
        page_builder.add(result)
      end

      def get_search_option(type, query, size, fields, sort)
        body = { }
        body[:query] = { query_string: { query: query } } unless query.nil?
        if sort
          sorts = []
          sort.each do |k, v|
            sorts << { k => v }
          end
          body[:sort] = sorts
        else
          body[:sort] = ["_doc"]
        end
        search_option = { index: @index_name, type: type, scroll: '1m', body: body, size: size }
        search_option[:_source] = fields.select{ |field| !field['metadata'] }.map { |field| field['name'] }.join(',')
        search_option
      end

      def get_sources(results, fields)
        hits = results['hits']['hits']
        hits.map { |hit|
          result = hit['_source']
          fields.select{ |field| field['metadata'] }.each { |field|
            result[field['name']] = hit[field['name']]
          }
          @fields.map { |field|
            convert_value(result[field['name']], field)
          }
        }
      end

      def convert_value(value, field)
        return nil if value.nil?
        case field["type"]
        when "string"
          value
        when "long"
          value.to_i
        when "double"
          value.to_f
        when "boolean"
          if value.is_a?(TrueClass) || value.is_a?(FalseClass)
            value
          else
            downcased_val = value.downcase
            case downcased_val
            when 'true' then true
            when 'false' then false
            when '1' then true
            when '0' then false
            else nil
            end
          end
        when "timestamp"
          Time.parse(value)
        when "json"
          value
        else
          raise "Unsupported type #{field['type']}"
        end
      end
    end
  end
end
