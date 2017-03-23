# Elasticsearch input plugin for Embulk [![Build Status](https://secure.travis-ci.org/toyama0919/embulk-input-elasticsearch.png?branch=master)](http://travis-ci.org/toyama0919/embulk-input-elasticsearch) [![Gem Version](https://badge.fury.io/rb/embulk-input-elasticsearch.svg)](http://badge.fury.io/rb/embulk-input-elasticsearch)

## Overview

* **Plugin type**: input
* **Resume supported**: yes
* **Cleanup supported**: yes
* **Guess supported**: no

## Configuration
- **nodes**: nodes (array, required)
  - **host**: host (string, required)
  - **port**: port (integer, required)
- **queries**: lucene query array. (array, required)
- **index**: index (string, required)
- **index_type**: index_type (string)
- **request_timeout**: request timeout (integer)
- **per_size**: per size query. (integer, required, default: `1000`)
- **limit_size**: limit size unit query. (integer, default: unlimit)
- **num_threads**: number of threads for queries. (integer, default: 1)
- **retry_on_failure**: retry on failure. set 0 is retry forever. (integer, default: 5)
- **sort**: sort order. (hash, default: nil)
- **scroll**: scroll. to keep the search context. (string, default: '1m')
- **fields**: fields (array, required)
  - **name**: name (string, required)
  - **type**: type (string, required)
  - **metadata**: metadata (boolean, default: false)
  - **time_format**: time_format (string)

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
  num_threads: 2
  sort:
    m_corporation_id: desc
    employee_range: asc
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
