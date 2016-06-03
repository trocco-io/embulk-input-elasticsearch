require 'excon'
require 'elasticsearch'

module Embulk
  module Input

    class Elasticsearch < InputPlugin
      Plugin.register_input("elasticsearch", self)

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
          "sort" => config.param("sort", :hash, default: nil)
        }

        columns = []
        task['fields'].each_with_index{ |field, i|
          columns << Column.new(i, field['name'], field['type'].to_sym)
        }

        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        task_reports = yield(task, columns, count)

        next_config_diff = {}
        return next_config_diff
      end

      def self.create_client(task)
        transport = ::Elasticsearch::Transport::Transport::HTTP::Faraday.new(
          {
            hosts: task['nodes'].map{ |node| Hash[node.map{ |k, v| [k.to_sym, v] }] },
            options: {
              reload_connections: task['reload_connections'],
              reload_on_failure: task['reload_on_failure'],
              retry_on_failure: task['retry_on_failure'],
              transport_options: {
                request: { timeout: task['request_timeout'] }
              }
            }
          }
        )

        ::Elasticsearch::Client.new transport: transport
      end

      def init
        @client = self.class.create_client(task)
        @index = task['index']
        @index_type = task['index_type']
        @queries = task['queries']
        @per_size = task['per_size']
        @limit_size = task['limit_size']
        @fields = task['fields']
        @sort = task['sort']
      end

      def run
        @queries.each do |query|
          query_count = 0
          no_source_results = search(@index_type, query, 0, 0, @routing, @fields, @sort)
          total_count = [no_source_results['hits']['total'], @limit_size].compact.min
          while true
            now_results_size = query_count * @per_size
            next_results_size = (query_count + 1) * @per_size
            size = get_size(next_results_size, now_results_size ,total_count)
            break if size == 0

            results = get_sources(search(@index_type, query, size, now_results_size, @routing, @fields, @sort), @fields)
            results.each do |record|
              page_builder.add(record)
            end
            break if last_query?(next_results_size ,total_count)
            query_count += 1
          end
        end
        page_builder.finish

        task_report = {}
        return task_report
      end

      private

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

      def get_size(next_results_size, now_results_size ,total_count)
        if last_query?(next_results_size ,total_count)
          (total_count - now_results_size)
        else
          @per_size
        end
      end

      def last_query?(next_results_size ,total_count)
        next_results_size > total_count
      end

      def search(type, query, size, from, routing, fields, sort)
        body = { from: from }
        body[:size] = size unless size.nil?
        if sort
          sorts = []
          sort.each do |k, v|
            sorts << { k => v }
          end
          body[:sort] = sorts
        end
        body[:query] = { query_string: { query: query } } unless query.nil?
        search_option = { index: @index, type: type, body: body }
        search_option[:routing] = routing unless routing.nil?
        search_option[:_source] = fields.select{ |field| !field['metadata'] }.map { |field| field['name'] }.join(',')
        Embulk.logger.info(%Q{search_option => #{search_option}})
        @client.search(search_option)
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
    end
  end
end
