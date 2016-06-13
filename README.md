# Elasticsearch input plugin for Embulk

## Overview

* **Plugin type**: input
* **Resume supported**: yes
* **Cleanup supported**: yes
* **Guess supported**: no

## Configuration
- **nodes**: nodes (array, required)
  - **host**: host (string, required)
  - **port**: port (integer, required)
- **queries**: query (array, required)
- **index**: index (string, required)
- **index_type**: index_type (string)
- **request_timeout**: request_timeout (integer)
- **per_size**: per_size (integer, required, default: `1000`)
- **limit_size**: limit_size (integer, default: unlimit)
- **fields**: fields (array, required)
  - **name**: name (string, required)
  - **type**: type (string, required)
  - **metadata**: metadata (boolean, default: false)
  - **time_format**: time_format (string, required)

## Example

```yaml
in:
  type: elasticsearch
  nodes:
    - {host: localhost, port: 9200}
  queries:
    - 'page_type: HP'
    - 'page_type: GP'
  index: crawl
  index_type: m_corporation_page
  request_timeout: 60
  per_size: 1000
  limit_size: 200000
  fields:
    - { name: _id, type: string, metadata: true }
    - { name: _type, type: string, metadata: true }
    - { name: _index, type: string, metadata: true }
    - { name: _score, type: double, metadata: true }
    - { name: page_type, type: string }
    - { name: corp_name, type: string }
    - { name: corp_key, type: string }
    - { name: title, type: string }
    - { name: body, type: string }
    - { name: url, type: string }
    - { name: employee_range, type: long }
    - { name: m_corporation_id, type: long }
    - { name: cg_lv1, type: json }
    - { name: cg_lv2, type: json }
    - { name: cg_lv3, type: json }
```

## Support Type
* string
* long
* double
* timestamp
* json
* boolean

## Build

```
$ rake
```
