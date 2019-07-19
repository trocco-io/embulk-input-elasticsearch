require 'excon'
require 'elasticsearch'

module Embulk
  module Input
    class Elasticsearch < InputPlugin
      class Connection
        def initialize(task)
          @scroll = task['scroll']
          @index = task['index']
          @index_type = task['index_type']
          @size = task['per_size']
          @fields = task['fields']
          @sort = task['sort']
          @limit_size = task['limit_size']
          @retry_on_failure = task['retry_on_failure']
          @client = create_client(
            nodes: task['nodes'],
            reload_connections: task['reload_connections'],
            reload_on_failure: task['reload_on_failure'],
            retry_on_failure: task['retry_on_failure'],
            request_timeout: task['request_timeout']
          )
        end

        def create_client(nodes: ,reload_connections: ,reload_on_failure: ,retry_on_failure: ,request_timeout:)
          transport = ::Elasticsearch::Transport::Transport::HTTP::Faraday.new(
            {
              hosts: nodes.map{ |node| Hash[node.map{ |k, v| [k.to_sym, v] }] },
              options: {
                reload_connections: reload_connections,
                reload_on_failure: reload_on_failure,
                retry_on_failure: retry_on_failure,
                transport_options: {
                  request: { timeout: request_timeout }
                }
              }
            }
          )

          ::Elasticsearch::Client.new transport: transport
        end

        def search_with_query(query)
          search_option = get_search_option(query)
          Embulk.logger.info("#{search_option}")
          r = search_with_retry { @client.search(search_option) }
          return if r.nil?
          i = 0
          Converter.get_sources(r, @fields).each do |result|
            yield(result) if block_given?
            return if @limit_size == (i += 1)
          end

          while r = (search_with_retry { @client.scroll(scroll_id: r['_scroll_id'], scroll: @scroll) }) and (not r['hits']['hits'].empty?) do
            Converter.get_sources(r, @fields).each do |result|
              yield(result) if block_given?
              return if @limit_size == (i += 1)
            end
          end
        end

        private

        def search_with_retry
          retries = 0
          begin
            yield if block_given?
          rescue => e
            if (@retry_on_failure == 0 || retries < @retry_on_failure)
              retries += 1
              Embulk.logger.warn "Could not search to Elasticsearch, resetting connection and trying again. #{e.message}"
              sleep 2**retries
              retry
            end
            msg = "Could not search to Elasticsearch after #{retries} retries. #{e.message}"
            raise Elasticsearch::ConfigError(e, msg)
          end
        end

        def get_search_option(query)
          body = { }
          body[:query] = { query_string: { query: query } } unless query.nil?
          if @sort
            sorts = []
            @sort.each do |k, v|
              sorts << { k => v }
            end
            body[:sort] = sorts
          else
            body[:sort] = ["_doc"]
          end
          search_option = { index: @index, type: @index_type, scroll: @scroll, body: body, size: @size }
          search_option[:_source] = @fields.select{ |field| !field['metadata'] }.map { |field| field['name'] }.join(',')
          search_option
        end
      end
    end
  end
end
