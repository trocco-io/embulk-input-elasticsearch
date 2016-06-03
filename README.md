# Elasticsearch input plugin for Embulk

## Overview

* **Plugin type**: input
* **Resume supported**: yes
* **Cleanup supported**: yes
* **Guess supported**: no

## Configuration
- **nodes**: nodes (array, required)
  - **host**: host (string, required, default: ``)
  - **port**: port (integer, required, default: ``)
- **queries**: query (array, required, default: ``)
- **index**: index (string, required, default: ``)
- **index_type**: index_type (string, default: ``)
- **request_timeout**: request_timeout (string, default: ``)
- **per_size**: per_size (integer, required, default: `1000`)
- **limit_size**: limit_size (integer, default: unlimit)
- **fields**: fields (string, required, default: ``)
  - **name**: name (string, required, default: ``)
  - **type**: type (string, required, default: ``)
  - **metadata**: metadata (boolean, required, default: ``)
  - **time_format**: time_format (string, required, default: ``)

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
