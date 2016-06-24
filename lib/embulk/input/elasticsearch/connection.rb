module Embulk
  module Input
    class Elasticsearch < InputPlugin
      class Connection
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
      end
    end
  end
end
