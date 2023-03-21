/* Copyright (c) 2016, 2022, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file sql/histograms/json_flex.cc
  Json_flex histogram (implementation).
*/

#include "sql/histograms/json_flex.h"

#include <algorithm>  // std::is_sorted
#include <iterator>
#include <new>

#include "field_types.h"  // enum_field_types
#include "my_base.h"      // ha_rows
#include "my_dbug.h"
#include "my_inttypes.h"
#include "mysql_time.h"
#include "sql-common/json_dom.h"       // Json_*
#include "sql/histograms/value_map.h"  // Value_map
#include "template_utils.h"

struct MEM_ROOT;

namespace histograms {

// Private constructor
Json_flex::Json_flex(MEM_ROOT *mem_root, const std::string &db_name,
                        const std::string &tbl_name,
                        const std::string &col_name, Value_map_type data_type,
                        bool *error)
    : Histogram(mem_root, db_name, tbl_name, col_name,
                enum_histogram_type::JSON_FLEX, data_type, error),
      m_buckets(mem_root) {}

// Public factory method
Json_flex *Json_flex::create(MEM_ROOT *mem_root,
                                   const std::string &db_name,
                                   const std::string &tbl_name,
                                   const std::string &col_name,
                                   Value_map_type data_type) {
  bool error = false;
  Json_flex *json_flex = new (mem_root)
      Json_flex(mem_root, db_name, tbl_name, col_name, data_type, &error);
  if (error) return nullptr;
  return json_flex;
}

Json_flex::Json_flex(MEM_ROOT *mem_root, const Json_flex &other,
                             bool *error)
    : Histogram(mem_root, other, error), m_buckets(mem_root) {
  /*
    Copy bucket contents. We need to make duplicates of String data, since they
    are allocated on a MEM_ROOT that most likely will be freed way too early.
  */
  if (m_buckets.reserve(other.m_buckets.size())) {
    *error = true;
    return;  // OOM
  }
  for (const auto &bucket : other.m_buckets) {
    char *string_data = bucket.value.dup(mem_root);
    if (string_data == nullptr) {
      *error = true;
      assert(false); /* purecov: deadcode */
      return;        // OOM
    }

    String string_dup(string_data, bucket.value.length(),
                      bucket.value.charset());
    m_buckets.push_back(
        JsonBucket(string_dup, bucket.cumulative_frequency));
  }
}

bool Json_flex::build_histogram(const Value_map<String> &value_map,
                                   size_t num_buckets) {
  // Clear any existing data.
  m_buckets.clear();
  m_null_values_fraction = INVALID_NULL_VALUES_FRACTION;
  m_sampling_rate = value_map.get_sampling_rate();

  // Set the number of buckets that was specified/requested by the user.
  m_num_buckets_specified = num_buckets;

  // Set the character set for the histogram data.
  m_charset = value_map.get_character_set();

  // Get total frequency count.
  ha_rows num_non_null_values = 0;
  for (const auto &node : value_map) num_non_null_values += node.second;

  // No values, nothing to do.
  if (num_non_null_values == 0) {
    if (value_map.get_num_null_values() > 0)
      m_null_values_fraction = 1.0;
    else
      m_null_values_fraction = 0.0;

    return false;
  }

  const ha_rows total_count =
      value_map.get_num_null_values() + num_non_null_values;

  // Set the fractions of NULL values.
  m_null_values_fraction =
      value_map.get_num_null_values() / static_cast<double>(total_count);

  // Create buckets with relative frequency, and not absolute frequency.
  ha_rows cumulative_sum = 0;

  if (m_buckets.reserve(value_map.size())) return true;  // OOM

  for (const auto &node : value_map) {
    cumulative_sum += node.second;
    const double cumulative_frequency =
        cumulative_sum / static_cast<double>(total_count);
    m_buckets.push_back(JsonBucket(node.first, cumulative_frequency));
  }

  return false;
}

bool Json_flex::histogram_to_json(Json_object *json_object) const {
  /*
    Call the base class implementation first. This will add the properties that
    are common among different histogram types, such as "last-updated" and
    "histogram-type".
  */
  if (Histogram::histogram_to_json(json_object))
    return true; /* purecov: inspected */

  // Add the Json_flex buckets.
  Json_array json_buckets;
  for (const auto &bucket : m_buckets) {
    Json_array json_bucket;
    if (create_json_bucket(bucket, &json_bucket))
      return true; /* purecov: inspected */
    if (json_buckets.append_clone(&json_bucket))
      return true; /* purecov: inspected */
  }

  if (json_object->add_clone(buckets_str(), &json_buckets))
    return true; /* purecov: inspected */

  if (histogram_data_type_to_json(json_object))
    return true; /* purecov: inspected */
  return false;
}

bool Json_flex::create_json_bucket(const JsonBucket &bucket,
                                      Json_array *json_bucket) {
  // Value
  if (add_value_json_bucket(bucket.value, json_bucket))
    return true; /* purecov: inspected */

  // Cumulative frequency
  const Json_double frequency(bucket.cumulative_frequency);
  if (json_bucket->append_clone(&frequency))
    return true; /* purecov: inspected */
  return false;
}


bool Json_flex::add_value_json_bucket(const String &value,
                                              Json_array *json_bucket) {
  const Json_opaque json_value(enum_field_types::MYSQL_TYPE_STRING, value.ptr(),
                               value.length());
  if (json_bucket->append_clone(&json_value))
    return true; /* purecov: inspected */
  return false;
}

std::string Json_flex::histogram_type_to_str() const {
  return json_flex_str();
}

bool Json_flex::json_to_histogram(const Json_object &json_object,
                                     Error_context *context) {
  if (Histogram::json_to_histogram(json_object, context)) return true;

  const Json_dom *buckets_dom = json_object.get(buckets_str());
  if (buckets_dom == nullptr) {
    context->report_missing_attribute(Histogram::buckets_str());
    return true;
  }
  if (buckets_dom->json_type() != enum_json_type::J_ARRAY) {
    context->report_node(buckets_dom, Message::JSON_WRONG_ATTRIBUTE_TYPE);
    return true;
  }

  const Json_array *buckets = down_cast<const Json_array *>(buckets_dom);
  if (m_buckets.reserve(buckets->size())) return true;  // OOM

  for (size_t i = 0; i < buckets->size(); ++i) {
    const Json_dom *bucket_dom = (*buckets)[i];
    if (buckets_dom == nullptr) {
      context->report_missing_attribute(Histogram::buckets_str());
      return true;
    }
    if (bucket_dom->json_type() != enum_json_type::J_ARRAY) {
      context->report_node(bucket_dom, Message::JSON_WRONG_ATTRIBUTE_TYPE);
      return true;
    }
    const Json_array *bucket = down_cast<const Json_array *>(bucket_dom);
    if (bucket->size() != 2) {
      context->report_node(bucket_dom, Message::JSON_WRONG_BUCKET_TYPE_2);
      return true;
    }

    // First item is the value, second is the cumulative frequency
    const Json_dom *cumulative_frequency_dom = (*bucket)[1];
    if (cumulative_frequency_dom->json_type() != enum_json_type::J_DOUBLE) {
      context->report_node(cumulative_frequency_dom,
                           Message::JSON_WRONG_ATTRIBUTE_TYPE);
      return true;
    }

    const Json_double *cumulative_frequency =
        down_cast<const Json_double *>(cumulative_frequency_dom);

    const Json_dom *value_dom = (*bucket)[0];
    String value;
    if (extract_json_dom_value(value_dom, &value, context)) return true;


    assert(m_buckets.capacity() > m_buckets.size());
    m_buckets.push_back(
        JsonBucket(value, cumulative_frequency->value()));
  }

  // Global post-check
  {
    /*
      Note that Json_flex may be built on an empty table or an all-NULL
      column. In this case the buckets array is empty.
    */
    if (m_buckets.empty()) {
      if (get_null_values_fraction() != 1.0 &&
          get_null_values_fraction() != 0.0) {
        context->report_global(Message::JSON_INVALID_NULL_VALUES_FRACTION);
        return true;
      }
    } else {
      JsonBucket *last_bucket = &m_buckets[m_buckets.size() - 1];
      float sum =
          last_bucket->cumulative_frequency + get_null_values_fraction();
      if (std::abs(sum - 1.0) > 0) {
        context->report_global(Message::JSON_INVALID_TOTAL_FREQUENCY);
        return true;
      }
    }
  }
  return false;
}

Histogram *Json_flex::clone(MEM_ROOT *mem_root) const {
  DBUG_EXECUTE_IF("fail_histogram_clone", return nullptr;);
  bool error = false;
  Histogram *json_flex = new (mem_root) Json_flex(mem_root, *this, &error);
  if (error) return nullptr;
  return json_flex;
}

double Json_flex::get_equal_to_selectivity(const String &value) const {
  if (value.length()) return 0.0;
  return 1.0;
}

double Json_flex::get_less_than_selectivity(const String &value) const {
  if (value.length()) return 0.0;
  return 1.0;
}

double Json_flex::get_greater_than_selectivity(const String &value) const {
  if (value.length()) return 0.0;
  return 1.0;
}

}  // namespace histograms
