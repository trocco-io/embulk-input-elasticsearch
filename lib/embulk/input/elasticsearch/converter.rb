module Embulk
  module Input
    class Elasticsearch < InputPlugin
      class Converter
        def self.get_sources(results, fields)
          hits = results['hits']['hits']
          hits.map { |hit|
            result = hit['_source']
            fields.map { |field|
              value = field['metadata'] ? hit[field['name']] : result[field['name']]
              convert_value(value, field)
            }
          }
        end

        def self.convert_value(value, field)
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
            raise Elasticsearch::TypecastError.new "Unsupported type #{field['type']}"
          end
        end
      end
    end
  end
end
