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
  Json flex histogram (implementation).
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
#include "template_utils.h"

struct MEM_ROOT;

namespace histograms {

// Private constructor
Json_flex::Json_flex(MEM_ROOT *mem_root, const std::string &db_name,
                        const std::string &tbl_name,
                        const std::string &col_name,
                        bool *error)
    : Histogram(mem_root, db_name, tbl_name, col_name,
                enum_histogram_type::JSON_FLEX, Value_map_type::INT, error),
                // TODO: remove superfluous value_map_type 
      m_buckets(mem_root) {}

// Public factory method
Json_flex *Json_flex::create(MEM_ROOT *mem_root,
                                   const std::string &db_name,
                                   const std::string &tbl_name,
                                   const std::string &col_name) {
  bool error = false;
  Json_flex *flex = new (mem_root)
      Json_flex(mem_root, db_name, tbl_name, col_name, &error);
  if (error) return nullptr;
  return flex;
}

Json_flex::Json_flex(MEM_ROOT *mem_root, const Json_flex &other,
                        bool *error)
    : Histogram(mem_root, other, error), m_buckets(mem_root) {
  if (m_buckets.reserve(other.m_buckets.size())) {
    *error = true;
    return;  // OOM
  }
  for (const auto &bucket : other.m_buckets) {
    m_buckets.push_back(bucket);
  }
}

bool Json_flex::build_histogram(size_t num_buckets) {
  // Shouldn't be called
  assert(false);
  return true && num_buckets;  // make compiler stop complaining
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
  // Key path
  const auto value = bucket.key_path; 
  const Json_opaque json_value(enum_field_types::MYSQL_TYPE_STRING, value.ptr(),
                               value.length());
  if (json_bucket->append_clone(&json_value))
    return true; /* purecov: inspected */

  // frequency
  const Json_double frequency(bucket.frequency);
  if (json_bucket->append_clone(&frequency))
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

    // That bucket exists and is an array
    if (buckets_dom == nullptr) {
      context->report_missing_attribute(Histogram::buckets_str());
      return true;
    }
    if (bucket_dom->json_type() != enum_json_type::J_ARRAY) {
      context->report_node(bucket_dom, Message::JSON_WRONG_ATTRIBUTE_TYPE);
      return true;
    }
    const Json_array *bucket = down_cast<const Json_array *>(bucket_dom);

    // Compute second item, the frequency
    const Json_dom *frequency_dom = (*bucket)[1];
    if (frequency_dom->json_type() != enum_json_type::J_DOUBLE) {
      context->report_node(frequency_dom,
                           Message::JSON_WRONG_ATTRIBUTE_TYPE);
      return true;
    }
    const Json_double *frequency =
        down_cast<const Json_double *>(frequency_dom);


    // Compute first item, the key path
    const Json_dom *key_path_dom = (*bucket)[0];
    String key_path;

    if (extract_json_dom_value(key_path_dom, &key_path, context)) {
      // Try the existing method first. If that fails, do our own simplified version instead
      if (key_path_dom->json_type() != enum_json_type::J_STRING) {
        context->report_node(frequency_dom,
                            Message::JSON_WRONG_ATTRIBUTE_TYPE);
        return true;
      }
      const Json_string *json_key_path = down_cast<const Json_string *>(key_path_dom);
      assert(get_character_set() != nullptr);
      key_path.set(json_key_path->value().c_str(), json_key_path->value().size(), get_character_set());
    }


    assert(m_buckets.capacity() > m_buckets.size());
    m_buckets.push_back(
        JsonBucket(key_path, frequency->value()));
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

double Json_flex::get_equal_to_selectivity(const longlong &value) const {
  if (!m_buckets.empty())
    return m_buckets.begin()->frequency;
  return 0.0 * value;
}

double Json_flex::get_less_than_selectivity(const longlong &value) const {
  if (!m_buckets.empty())
    return m_buckets.begin()->frequency;
  return 0.0 * value;
}

double Json_flex::get_greater_than_selectivity(const longlong &value) const {
  if (!m_buckets.empty())
    return m_buckets.begin()->frequency;
  return 0.0 * value;
}

}  // namespace histograms
