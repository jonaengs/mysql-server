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
#include <any>

#include "field_types.h"  // enum_field_types
#include "my_base.h"      // ha_rows
#include "my_dbug.h"
#include "my_inttypes.h"
#include "mysql_time.h"
#include "sql-common/json_dom.h"       // Json_*
#include "sql/histograms/value_map.h"  // Value_map
#include "template_utils.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/item_json_func.h"

struct MEM_ROOT;

namespace histograms {

// Private constructor
Json_flex::Json_flex(MEM_ROOT *mem_root, const std::string &db_name,
                        const std::string &tbl_name,
                        const std::string &col_name,
                        bool *error)
    : Histogram(mem_root, db_name, tbl_name, col_name,
                enum_histogram_type::JSON_FLEX, Value_map_type::JSON, error),
      m_buckets(mem_root) {}

// Public factory method
Json_flex *Json_flex::create(MEM_ROOT *mem_root,
                                   const std::string &db_name,
                                   const std::string &tbl_name,
                                   const std::string &col_name) {
  bool error = false;
  Json_flex *json_flex = new (mem_root)
      Json_flex(mem_root, db_name, tbl_name, col_name, &error);
  if (error) return nullptr;
  return json_flex;
}

template<typename T>
JsonGram<T> *JsonGram<T>::create_singlegram(MEM_ROOT *mem_root) {
    return new (mem_root) JsonGram<T>{JFlexHistType::SINGLETON, Mem_root_array<SingleBucket>(mem_root)};
}
template<typename T>
JsonGram<T> *JsonGram<T>::create_equigram(MEM_ROOT *mem_root) {
    return new (mem_root) JsonGram<T>{JFlexHistType::EQUI_HEIGHT, Mem_root_array<SingleBucket>(mem_root)};
}

template<>
JsonGram<BucketString> *JsonGram<BucketString>::duplicate_onto(MEM_ROOT *mem_root) {
  auto *dupe = copy_struct(mem_root);
  if (dupe->buckets_type == JFlexHistType::SINGLETON) {
    dupe->m_buckets.single_bucks.reserve(m_buckets.single_bucks.size());

    for (const auto &bucket : m_buckets.single_bucks) {
        BucketString bs;
        if (bucket.value.dupe(mem_root, &bs)) return nullptr;

        auto duped_bucket = JsonGram<BucketString>::SingleBucket{bs, bucket.frequency};
        dupe->m_buckets.single_bucks.push_back(duped_bucket);
    }
  } else {
    dupe->m_buckets.equi_bucks.reserve(m_buckets.equi_bucks.size());

    for (const auto &bucket : m_buckets.equi_bucks) {
        BucketString bs;
        if (bucket.upper_bound.dupe(mem_root, &bs)) return nullptr;

        auto duped_bucket = JsonGram<BucketString>::EquiBucket{bs, bucket.frequency, bucket.ndv};
        dupe->m_buckets.equi_bucks.push_back(duped_bucket);
    }
  }
  dupe->rest_mean_frequency = rest_mean_frequency;
  return dupe;
}

template<typename T>
JsonGram<T> *JsonGram<T>::duplicate_onto(MEM_ROOT *mem_root) {
  auto *dupe = copy_struct(mem_root);
  if (dupe->buckets_type == JFlexHistType::SINGLETON) {
    dupe->m_buckets.single_bucks.reserve(m_buckets.single_bucks.size());    
    for (const auto &bucket : m_buckets.single_bucks) {
      dupe->m_buckets.single_bucks.emplace_back(bucket);
    }
  } else {
    dupe->m_buckets.equi_bucks.reserve(m_buckets.equi_bucks.size());
    for (const auto &bucket : m_buckets.equi_bucks) {
      dupe->m_buckets.equi_bucks.emplace_back(bucket);
    }
  }
  return dupe;
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
    char *string_data = bucket.key_path.dup(mem_root);
    if (string_data == nullptr) {
      *error = true;
      assert(false); /* purecov: deadcode */
      return;        // OOM
    }

    String string_dup(string_data, bucket.key_path.length(),
                      bucket.key_path.charset());


    // Duplicate min/max data when strings
    maybe_primitive min_val_copy = bucket.min_val;
    maybe_primitive max_val_copy = bucket.max_val;
    if (bucket.values_type == BucketValueType::STRING) {
      (*min_val_copy)._string = BucketString::dupe(mem_root, (*min_val_copy)._string);
      (*max_val_copy)._string = BucketString::dupe(mem_root, (*max_val_copy)._string);

      // Sanity checks
      {
        // Check that pointers are not the same
        assert((*min_val_copy)._string.m_ptr != (*bucket.min_val)._string.m_ptr);
        assert((*max_val_copy)._string.m_ptr != (*bucket.max_val)._string.m_ptr);
        // Check that string values are the same
        
        String min_cpy = (*min_val_copy)._string.to_string();
        String max_cpy = (*max_val_copy)._string.to_string();
        String min_original = (*bucket.min_val)._string.to_string();
        String max_original = (*bucket.max_val)._string.to_string();
        assert(stringcmp(&min_cpy, &min_original) == 0);
        assert(stringcmp(&max_cpy, &max_original) == 0);
        assert(stringcmp(&min_original, &max_original) == stringcmp(&min_cpy, &max_cpy));
      }
    }
    

    // Duplicate histogram data
    void *json_gram = nullptr;
    if (bucket.histogram) {
      switch (bucket.values_type) {
        case BucketValueType::INT: {
          json_gram = static_cast<JsonGram<longlong> *>(bucket.histogram)->duplicate_onto(mem_root);
          break;
        }
        case BucketValueType::FLOAT: {
          json_gram = static_cast<JsonGram<double> *>(bucket.histogram)->duplicate_onto(mem_root);
          break;
        }
        case BucketValueType::BOOL: {
          json_gram = static_cast<JsonGram<bool> *>(bucket.histogram)->duplicate_onto(mem_root);
          break;
        }
        case BucketValueType::STRING: {
          json_gram = static_cast<JsonGram<BucketString> *>(bucket.histogram)->duplicate_onto(mem_root);
          break;
        }
        case BucketValueType::UNKNOWN: {
          assert(false);
          *error = true;
          return;
        }
      }
    }

    JsonBucket copy(string_dup, bucket.frequency, bucket.null_values,
                    min_val_copy, max_val_copy, bucket.ndv,
                    bucket.values_type, json_gram);

    if (bucket.values_type != BucketValueType::UNKNOWN) {
      // If one of the optionals is included, the other should be as well
      assert(bucket.min_val && bucket.max_val);
      assert(copy.min_val && copy.max_val);
      // Copied values should match
      // Except for strings, which contain pointers
      if (bucket.values_type != BucketValueType::STRING) {
        assert((*copy.min_val)._int == (*bucket.min_val)._int);
        assert((*copy.max_val)._int == (*bucket.max_val)._int);
      }


      // Ndv should be in etiher both or none
      assert(!bucket.ndv == !copy.ndv);
      // If ndv is included, the value of original and copy should match. And min and max must be included
      if (copy.ndv) {
        assert(bucket.ndv == copy.ndv);
        // If ndv is included, min and max vals should be as well
        assert(copy.max_val && copy.min_val);
      }

      // None or both have histograms
      assert(!bucket.histogram == !copy.histogram);
      // If histogram is included, then ndv must be included
      if (copy.histogram) {
        assert(copy.ndv);
      }
    } else {
      // If the value type is unknown, the optionals should not be set
      assert(!bucket.min_val && !bucket.max_val);
      assert(!copy.min_val && !copy.max_val);
      assert(!bucket.ndv && !copy.ndv);
      assert(!bucket.histogram && !copy.histogram);
    }

    m_buckets.push_back(copy);
  }
}


bool Json_flex::histogram_to_json(Json_object *json_object) const {
  /*
    Call the base class implementation first. This will add the properties that
    are common among different histogram types, such as "last-updated" and
    "histogram-type".
  */
  if (Histogram::histogram_to_json(json_object))
    return true;

  // Add the Json_flex buckets.
  Json_array json_buckets;
  for (const auto &bucket : m_buckets) {
    Json_array json_bucket;
    if (create_json_bucket(bucket, &json_bucket))
      return true;
    if (json_buckets.append_clone(&json_bucket))
      return true;
  }

  if (json_object->add_clone(buckets_str(), &json_buckets))
    return true;

  if (histogram_data_type_to_json(json_object))
    return true;
  return false;
}

std::string s1("Test1");
std::string s2("ABCDEF");

template<typename T>
bool JsonGram<T>::populate_json_array(Json_array *buckets_array) {
  if (buckets_type == JFlexHistType::SINGLETON) {
    for (const auto &bucket : m_buckets.single_bucks) {
      Json_array json_bucket;
      if (Json_flex::add_value_json_bucket(bucket.value, &json_bucket)) return true;
      if (Json_flex::add_value_json_bucket(bucket.frequency, &json_bucket)) return true;
      
      if (buckets_array->append_clone(&json_bucket)) return true;
    }
    
    assert(buckets_array->size() == m_buckets.single_bucks.size());
  } else {
    for (const auto &bucket : m_buckets.equi_bucks) {
      Json_array json_bucket;
      if (Json_flex::add_value_json_bucket(bucket.upper_bound, &json_bucket)) return true;
      if (Json_flex::add_value_json_bucket(bucket.frequency, &json_bucket)) return true;
      if (Json_flex::add_value_json_bucket(bucket.ndv, &json_bucket)) return true;
      
      if (buckets_array->append_clone(&json_bucket)) return true;
    }
    
    assert(buckets_array->size() == m_buckets.equi_bucks.size());
  }


  return false;
}

bool Json_flex::create_json_bucket(const JsonBucket &bucket,
                                      Json_array *json_bucket) {

  // Key path
  if (add_value_json_bucket(bucket.key_path, json_bucket))
    return true;

  // frequency
  if (add_value_json_bucket(bucket.frequency, json_bucket))
    return true;

  // null_values
  if (add_value_json_bucket(bucket.null_values, json_bucket))
    return true;


  // this is broken for doubles. TODO: FIX
  // Assume that in min_val is defined, then max_val will be as well
  if (bucket.values_type != BucketValueType::UNKNOWN) {
    switch (bucket.values_type) {
      case BucketValueType::INT: {
        if (add_value_json_bucket((*bucket.min_val)._int, json_bucket)) return true;
        if (add_value_json_bucket((*bucket.max_val)._int, json_bucket)) return true;
        break;
      }
      case BucketValueType::FLOAT: {
        if (add_value_json_bucket((*bucket.min_val)._float, json_bucket)) return true;
        if (add_value_json_bucket((*bucket.max_val)._float, json_bucket)) return true;
        break;
      }
      case BucketValueType::BOOL: {
        if (add_value_json_bucket((*bucket.min_val)._bool, json_bucket)) return true;
        if (add_value_json_bucket((*bucket.max_val)._bool, json_bucket)) return true;
        break;
      }
      case BucketValueType::STRING: {
        if (add_value_json_bucket((*bucket.min_val)._string, json_bucket)) return true;
        if (add_value_json_bucket((*bucket.max_val)._string, json_bucket)) return true;
        break;
      }
      case BucketValueType::UNKNOWN: {
        // unreachable
        assert(false);
        return true;
      }
    }
    
    // Add NDV
    if (bucket.ndv) {
      if (add_value_json_bucket(*bucket.ndv, json_bucket)) return true;

      // Add Histogram
      if (bucket.histogram) {
        Json_object json_gram;

        // Add type string to object
        // Cast to JsonGram<void> becuase actual type of json_gram does not matter when calling the get_bucket_type_str method
        const Json_string json_gram_type(
          static_cast<JsonGram<void> *>(bucket.histogram)->get_bucket_type_str());
        if (json_gram.add_clone(JsonGram<void>::type_str(), &json_gram_type))
          return true;

        // Populate buckets array
        Json_array buckets_arr;
        switch (bucket.values_type) {
          case BucketValueType::INT: {
            static_cast<JsonGram<longlong> *>(bucket.histogram)->populate_json_array(&buckets_arr);
            break;
          }
          case BucketValueType::FLOAT: {
            static_cast<JsonGram<double> *>(bucket.histogram)->populate_json_array(&buckets_arr);
            break;
          }
          case BucketValueType::BOOL: {
            static_cast<JsonGram<bool> *>(bucket.histogram)->populate_json_array(&buckets_arr);
            break;
          }
          case BucketValueType::STRING: {
            static_cast<JsonGram<BucketString> *>(bucket.histogram)->populate_json_array(&buckets_arr);
            break;
          }
          case BucketValueType::UNKNOWN: {
            // again, unreachable
            assert(false);
            return true;
          }
        }

        // Add buckets array to object
        if (json_gram.add_clone(buckets_str(), &buckets_arr)) return true;

        // Add mean rest frequency if set
        auto *void_jgram = static_cast<JsonGram<void> *>(bucket.histogram);
        if (void_jgram->rest_mean_frequency) {
          const Json_double mean_frequency(*(void_jgram->rest_mean_frequency));
          if (json_gram.add_clone(void_jgram->rest_frequency_string(), &mean_frequency)) {
            return true;  
          } 
        }

        // Add json_gram to json bucket
        if (json_bucket->append_clone(&json_gram)) return true;
      }
    }
  }

  
  return false;
}


bool Json_flex::add_value_json_bucket(const BucketString &value,
                                              Json_array *json_bucket) {

  const Json_opaque json_value(enum_field_types::MYSQL_TYPE_STRING, value.m_ptr,
                               value.m_length);
  return json_bucket->append_clone(&json_value);
}

bool Json_flex::add_value_json_bucket(const String &value,
                                              Json_array *json_bucket) {
  const Json_opaque json_value(enum_field_types::MYSQL_TYPE_STRING, value.ptr(),
                               value.length());
  return json_bucket->append_clone(&json_value);
}

bool Json_flex::add_value_json_bucket(const double &value,
                                              Json_array *json_bucket) {
  const Json_double json_value(value);
  return json_bucket->append_clone(&json_value);
}

bool Json_flex::add_value_json_bucket(const longlong &value,
                                              Json_array *json_bucket) {
  const Json_int json_value(value);
  return json_bucket->append_clone(&json_value);
}
bool Json_flex::add_value_json_bucket(const bool &value,
                                              Json_array *json_bucket) {
  const Json_boolean json_value(value);
  return json_bucket->append_clone(&json_value);
}

std::string Json_flex::histogram_type_to_str() const {
  return json_flex_str();
}

template<typename T>
bool JsonGram<T>::json_to_json_gram(const Json_array *buckets_array, Json_flex *histogram, Error_context *context) {
  // determine number of buckets
  size_t num_buckets = buckets_array->size();

  // reserve space for the buckets on mem_root
  if (buckets_type == JFlexHistType::SINGLETON) {
    m_buckets.single_bucks.reserve(num_buckets);
  } else {
    m_buckets.equi_bucks.reserve(num_buckets);
  }

  // Copy bucket contents into the JsonGram
  // Loop over buckets, copying one value at a time
  for (size_t i = 0; i < num_buckets; ++i) {
    const Json_dom *bucket_dom = (*buckets_array)[i];
    if (bucket_dom == nullptr) {
      context->report_missing_attribute("aaaaaaa");
      return true;
    }
    if (bucket_dom->json_type() != enum_json_type::J_ARRAY) {
      context->report_node(bucket_dom, Message::JSON_WRONG_ATTRIBUTE_TYPE);
      return true;
    }
    const Json_array *bucket = down_cast<const Json_array *>(bucket_dom);
    if (buckets_type == JFlexHistType::SINGLETON) {
      assert(bucket->size() == 2);
    } else {
      assert(bucket->size() == 3);
    }
    

    const Json_dom *value_dom = (*bucket)[0];    
    T value;
    if (histogram->extract_json_dom_value(value_dom, &value, context)) return true;


    const Json_dom *frequency_dom = (*bucket)[1];
    double frequency;
    if (histogram->extract_json_dom_value(frequency_dom, &frequency, context)) return true;

          
    if (buckets_type == JFlexHistType::SINGLETON) {
      // Create bucket and add to array
      SingleBucket single_bucket = SingleBucket{value, frequency};
      assert(m_buckets.single_bucks.capacity() > m_buckets.single_bucks.size());
      m_buckets.single_bucks.push_back(single_bucket);
    } else {
      // Extract third value only present in equi buckets
      const Json_dom *ndv_dom = (*bucket)[2];
      longlong ndv;
      if (histogram->extract_json_dom_value(ndv_dom, &ndv, context)) return true;

      // Create bucket and add to array
      EquiBucket equi_bucket = EquiBucket{value, frequency, ndv};
      assert(m_buckets.equi_bucks.capacity() > m_buckets.equi_bucks.size());
      m_buckets.equi_bucks.push_back(equi_bucket);
    }
  }

  return false;
}


bool Json_flex::json_to_histogram(const Json_object &json_object,
                                     Error_context *context) {
  if (Histogram::json_to_histogram(json_object, context)) return true;

  // VERIFY BUCKETS JSON & ALLOCATE BUCKETS MEMORY
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

  // COPY BUCKET VALUES
  for (size_t i = 0; i < buckets->size(); ++i) {
    // VERIFY BUCKET JSON
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
    const size_t allowed_size_wo_opts = json_bucket_total_member_count() - json_bucket_optional_member_count();
    // Either all or none of the optional values must be provided
    if (bucket->size() < allowed_size_wo_opts) {
      context->report_node(bucket_dom, Message::JSON_WRONG_BUCKET_TYPE_N);
      return true;
    }


    // GET FIRST BUCKET ITEM: key_path
    const Json_dom *key_path_dom = (*bucket)[0];
    String key_path;
    if (extract_json_dom_value(key_path_dom, &key_path, context)) return true;


    // GET SECOND BUCKET ITEM: frequency
    const Json_dom *frequency_dom = (*bucket)[1];
    double frequency;
    if (extract_json_dom_value(frequency_dom, &frequency, context)) return true;
    min_frequency = std::min(min_frequency, frequency); // Set minimum frequency found across all buckets

    // GET THIRD BUCKET ITEM: null_values
    const Json_dom *null_values_dom = (*bucket)[2];
    double null_values;
    if (extract_json_dom_value(null_values_dom, &null_values, context)) return true;


    std::optional<json_primitive> min_val_opt;
    std::optional<json_primitive> max_val_opt;
    std::optional<longlong> ndv_opt;
    BucketValueType values_type = BucketValueType::UNKNOWN;
    if (bucket->size() >= allowed_size_wo_opts + 2) {
      // GET FOURTH BUCKET ITEM: min_val
      // GET FIFTH BUCKET ITEM:  max_val
      const Json_dom *min_val_dom = (*bucket)[3];
      const Json_dom *max_val_dom = (*bucket)[4];
      json_primitive min_val;
      json_primitive max_val;

      if (min_val_dom->json_type() != max_val_dom->json_type()) {
        context->report_node(bucket_dom, Message::JSON_WRONG_ATTRIBUTE_TYPE);
        return true;
      }
      if (min_val_dom->json_type() == enum_json_type::J_DOUBLE) {
        if (extract_json_dom_value(min_val_dom, &(min_val._float), context)) return true;
        if (extract_json_dom_value(max_val_dom, &(max_val._float), context)) return true;
        values_type = BucketValueType::FLOAT;
      } else if (min_val_dom->json_type() == enum_json_type::J_BOOLEAN) {
        if (extract_json_dom_value(min_val_dom, &min_val._bool, context)) return true;
        if (extract_json_dom_value(max_val_dom, &max_val._bool, context)) return true;
        values_type = BucketValueType::BOOL;
      }
      else if (min_val_dom->json_type() == enum_json_type::J_INT ||
                 min_val_dom->json_type() == enum_json_type::J_UINT) {
        if (extract_json_dom_value(min_val_dom, &min_val._int, context)) return true;
        if (extract_json_dom_value(max_val_dom, &max_val._int, context)) return true;
        values_type = BucketValueType::INT;
      }
      else if (min_val_dom->json_type() == enum_json_type::J_STRING ||
               min_val_dom->json_type() == enum_json_type::J_OPAQUE) {
        String min_val_str;
        String max_val_str;
        if (extract_json_dom_value(min_val_dom, &min_val_str, context)) return true;
        if (extract_json_dom_value(max_val_dom, &max_val_str, context)) return true;
        min_val._string = BucketString::from_string(min_val_str);
        max_val._string = BucketString::from_string(max_val_str);
        values_type = BucketValueType::STRING;
      }
      else {
        assert(false);
        context->report_node(bucket_dom, Message::JSON_WRONG_ATTRIBUTE_TYPE);
        return true;
      }


      min_val_opt = min_val;
      max_val_opt = max_val;

      assert(min_val_opt);
      assert(max_val_opt);
      assert(values_type != BucketValueType::UNKNOWN);
    }

    if (bucket->size() >= allowed_size_wo_opts + 3) {
      // GET SIXTH BUCKET: NDV
      const Json_dom *ndv_dom = (*bucket)[5];
      longlong ndv;
      if (extract_json_dom_value(ndv_dom, &ndv, context)) return true;
      ndv_opt = ndv;
    }

    void *json_gram = nullptr;
    if (bucket->size() >= allowed_size_wo_opts + 4) {
      // GET SEVENTH BUCKET: JsonGram
      assert(values_type != BucketValueType::UNKNOWN);

      // Missing type checks inbound
      const Json_dom *histogram_object_dom = (*bucket)[6];
      const Json_object *histogram_object = down_cast<const Json_object *>(histogram_object_dom);
      const Json_dom *hist_type_dom = histogram_object->get(JsonGram<void>::type_str());
      if (hist_type_dom == nullptr) {
        context->report_missing_attribute(JsonGram<void>::type_str());
        return true;
      }
      const Json_dom *hist_buckets_dom = histogram_object->get(buckets_str());
      if (hist_buckets_dom == nullptr) {
        context->report_missing_attribute(Histogram::buckets_str());
        return true;
      }

      // determine histogram's bucket type
      const Json_string *hist_type_str = down_cast<const Json_string *>(hist_type_dom);
      assert(hist_type_str->value() == "singleton" || hist_type_str->value() == "equi-height");
      auto bucket_type = hist_type_str->value() == "singleton" ? 
        JFlexHistType::SINGLETON : JFlexHistType::EQUI_HEIGHT;


      // Create JsonGram. T is determined from values_type
      const Json_array *buckets_array = down_cast<const Json_array *>(hist_buckets_dom);
      switch (values_type) {
        case BucketValueType::INT: {
          if (bucket_type == JFlexHistType::SINGLETON) {
            json_gram = static_cast<void *>(JsonGram<longlong>::create_singlegram(get_mem_root()));
          } else {
            json_gram = static_cast<void *>(JsonGram<longlong>::create_equigram(get_mem_root()));
          }
          static_cast<JsonGram<longlong> *>(json_gram)->json_to_json_gram(buckets_array, this, context);
          break;
        }
        case BucketValueType::FLOAT: {
          if (bucket_type == JFlexHistType::SINGLETON) {
            json_gram = static_cast<void *>(JsonGram<double>::create_singlegram(get_mem_root()));
          } else {
            json_gram = static_cast<void *>(JsonGram<double>::create_equigram(get_mem_root()));
          }
          static_cast<JsonGram<double> *>(json_gram)->json_to_json_gram(buckets_array, this, context);
          break;
        }
        case BucketValueType::BOOL: {
          if (bucket_type == JFlexHistType::SINGLETON) {
            json_gram = static_cast<void *>(JsonGram<bool>::create_singlegram(get_mem_root()));
          } else {
            json_gram = static_cast<void *>(JsonGram<bool>::create_equigram(get_mem_root()));
          }
          static_cast<JsonGram<bool> *>(json_gram)->json_to_json_gram(buckets_array, this, context);
          break;
        }
        case BucketValueType::STRING: {
          if (bucket_type == JFlexHistType::SINGLETON) {
            json_gram = static_cast<void *>(JsonGram<BucketString>::create_singlegram(get_mem_root()));
          } else {
            json_gram = static_cast<void *>(JsonGram<BucketString>::create_equigram(get_mem_root()));
          }
          static_cast<JsonGram<BucketString> *>(json_gram)->json_to_json_gram(buckets_array, this, context);
          break;
        }
        case BucketValueType::UNKNOWN: {
          assert(false);
          context->report_node(bucket_dom, Message::JSON_WRONG_ATTRIBUTE_TYPE);
          return true;
        }
      }
      const Json_dom *rest_frequency_dom = histogram_object->get(JsonGram<void>::rest_frequency_string());
      if (rest_frequency_dom) {
        // TODO: Proper err here
        assert(rest_frequency_dom->json_type() == enum_json_type::J_DOUBLE);
        const Json_double *rest_frequncy_json = down_cast<const Json_double *>(rest_frequency_dom);
        static_cast<JsonGram<void> *>(json_gram)->rest_mean_frequency = rest_frequncy_json->value();
      }
    }
    

    // STORE BUCKET IN HISTOGRAM
    JsonBucket hist_bucket = JsonBucket(key_path, frequency, null_values, min_val_opt, max_val_opt, ndv_opt, values_type, json_gram);
    assert(m_buckets.capacity() > m_buckets.size());
    m_buckets.push_back(hist_bucket);
  }
  return false;

  /*
  Potential verification checks:
  * If NDV == 1, check that min_val == max_val. If NDV > 1, check that min_val != max_val
  * If a histogram is present, check that NDV >= histogram_length
  * Check that frequency, null_values/null_frequency in [0, 1]
  * Check that each key_path has a valid format
  * Check that string value lengths do not exceed MAX_FIELD_WITH
  
  */
}

Histogram *Json_flex::clone(MEM_ROOT *mem_root) const {
  DBUG_EXECUTE_IF("fail_histogram_clone", return nullptr;);
  bool error = false;
  Histogram *json_flex = new (mem_root) Json_flex(mem_root, *this, &error);
  static_cast<Json_flex *>(json_flex)->min_frequency = min_frequency;
  if (error) return nullptr;
  return json_flex;
}


// Eye-catching value that's returned from functions which return selectivity
// and which shouldn't be able to fail but which somehow did fail.
const double err_selectivity_val = 0.123456789;

template<typename T>
double selectivity_getter_dispatch(const Json_flex *jflex, const String &arg_path, enum_operator op, T value) {
  // For now, treat GT & GTE and LT & GTE the same
  // Will obviously lead to errors in some cases, but the error is unlikely to be worse than 
  // if we simply didn't support the GTE and LTE operations
  switch(op) {
    case enum_operator::EQUALS_TO: {
      return jflex->get_equal_to_selectivity(arg_path, value);
    }
    case enum_operator::NOT_EQUALS_TO: {
      return jflex->get_not_equal_to_selectivity(arg_path, value);
    }
    case enum_operator::LESS_THAN_OR_EQUAL:
    case enum_operator::LESS_THAN: {
      return jflex->get_less_than_selectivity(arg_path, value);
    }
    case enum_operator::GREATER_THAN_OR_EQUAL:
    case enum_operator::GREATER_THAN: {
      return jflex->get_greater_than_selectivity(arg_path, value);
    }
    default: {
      assert(false);
      return err_selectivity_val;
    }
  }
}

double selectivity_getter_dispatch(const Json_flex *jflex, const String &arg_path, enum_operator op) {
  // For now, treat GT & GTE and LT & GTE the same
  switch(op) {
    case enum_operator::NOT_EQUALS_TO: {
      return jflex->get_not_equal_to_selectivity(arg_path);
    }
    case enum_operator::EQUALS_TO: {
      return jflex->get_equal_to_selectivity(arg_path);
    }
    case enum_operator::LESS_THAN_OR_EQUAL:
    case enum_operator::LESS_THAN: {
      return jflex->get_less_than_selectivity(arg_path);
    }
    case enum_operator::GREATER_THAN_OR_EQUAL:
    case enum_operator::GREATER_THAN: {
      return jflex->get_greater_than_selectivity(arg_path);
    }
    default: {
      assert(false); 
      return err_selectivity_val;
    }
  }
}

// Requires that only the permitted (i.e., handled in switch) operators are sent
// And requires that all comparands are of the same type
double multi_val_dispatch(const Json_flex *jflex, const String &arg_path,
                            enum_operator op, Item **comparands, size_t comparand_count) {
  switch(op) {
    case enum_operator::BETWEEN: {
      // Calculating BETWEEN (BETWEEN is inclusive in both ends):
      // Sum everything outside the range (using lt and gt). Return 1 minus that sum.

      assert(comparand_count == 2);  // Pretty sure this is checked way before we get here, but I'm leaving it just in case
      
      switch(comparands[0]->type()) {
        case Item::Type::INT_ITEM: {
          // For now, assume that the smaller item always comes first
          assert(comparands[0]->val_int() <= comparands[1]->val_int());
          auto below = jflex->get_less_than_selectivity(arg_path, comparands[0]->val_int());
          auto above = jflex->get_greater_than_selectivity(arg_path, comparands[1]->val_int());
          return 1 - (above + below);
        }
        case Item::Type::REAL_ITEM: {
          // For now, assume that the smaller item always comes first
          assert(comparands[0]->val_real() <= comparands[1]->val_real());
          auto below = jflex->get_less_than_selectivity(arg_path, comparands[0]->val_real());
          auto above = jflex->get_greater_than_selectivity(arg_path, comparands[1]->val_real());
          return 1 - (above + below);
        }
        case Item::Type::STRING_ITEM: {
          StringBuffer<MAX_FIELD_WIDTH> below_str_buf(comparands[0]->collation.collation);
          const String *below_str = comparands[0]->val_str(&below_str_buf);
          // const String below_truncated = below_str->substr(0, HISTOGRAM_MAX_COMPARE_LENGTH);
          
          StringBuffer<MAX_FIELD_WIDTH> above_str_buf(comparands[1]->collation.collation);
          const String *above_str = comparands[1]->val_str(&above_str_buf);
          // const String above_truncated = above_str->substr(0, HISTOGRAM_MAX_COMPARE_LENGTH);
          
          // For now, assume that the smaller item always comes first
          // assert(stringcmp(&below_truncated, &above_truncated) <= 0);
          assert(stringcmp(below_str, above_str) <= 0);
          
          // auto below = jflex->get_less_than_selectivity(arg_path, below_truncated);
          // auto above = jflex->get_greater_than_selectivity(arg_path, above_truncated);
          auto below = jflex->get_less_than_selectivity(arg_path, *below_str);
          auto above = jflex->get_greater_than_selectivity(arg_path, *above_str);
          
          return 1 - (above + below);
        }
        default: {
          // Assume only numbers or strings in BETWEEN queries
          assert(false);
          return err_selectivity_val;
        }
      }
    }
    case enum_operator::IN_LIST: {
      // Calculation is very simple: Sum up equality for every match
      // Very inefficient if the list is large -- Can be done in a single lookup rather than |list| lookups
      switch(comparands[0]->type()) {
        case Item::Type::INT_ITEM: {
          double sum = 0;
          for (size_t i = 0; i < comparand_count; i++) {
            sum += jflex->get_equal_to_selectivity(arg_path, comparands[i]->val_int());
          }
          if (auto bucketOpt = jflex->find_bucket(arg_path)) {
            // Bound sum by the complete frequency of the bucket
            return std::min((*bucketOpt)->frequency, sum);
          }
          return sum;
        }
        case Item::Type::STRING_ITEM: {
          double sum = 0;
          for (size_t i = 0; i < comparand_count; i++) {
            StringBuffer<MAX_FIELD_WIDTH> str_buf(comparands[i]->collation.collation);
            const String *str_val = comparands[i]->val_str(&str_buf);
            // const String truncated = str_val->substr(0, HISTOGRAM_MAX_COMPARE_LENGTH);
            sum += jflex->get_equal_to_selectivity(arg_path, *str_val);
          }
          if (auto bucketOpt = jflex->find_bucket(arg_path)) {
            return std::min((*bucketOpt)->frequency, sum);
          }
          return sum;
        }
        case Item::Type::FUNC_ITEM: {
          double sum = 0;
          for (size_t i = 0; i < comparand_count; i++) {
            if (comparands[i]->val_int() == 1 || comparands[i]->val_int() == 0) {
              sum += jflex->get_equal_to_selectivity(arg_path, comparands[i]->val_bool());
            } else {
              assert(false);
              return err_selectivity_val;
            }
          }
          if (auto bucketOpt = jflex->find_bucket(arg_path)) {
            return std::min((*bucketOpt)->frequency, sum);
          }
          return sum;
        }
        default: {
          // For now, assume no float or bool lists
          assert(false);
          return err_selectivity_val;
        }
      }
    }
    case enum_operator::NOT_IN_LIST: {
      double in_list_selectivity = multi_val_dispatch(
        jflex, arg_path, enum_operator::IN_LIST, comparands, comparand_count
      );
      double total_selectivity = jflex->get_not_eq_null_selectivity(arg_path);
      return total_selectivity - in_list_selectivity;
    }

    default: {
      assert(false);
      return err_selectivity_val;
    }
  }
}

bool get_json_func_path_item(const Item_func *func, Item **json_path_arg) {
  // Find the innermost function in the (potentially) nested set of function calls.
  // Currently, we're just assuming that the functions passed here are always JSON funcs. TODO: Find a way to check for JSON functions
  const Item_json_func *innermost_func;

  if (func->func_name() == std::string("json_unquote")) {
    innermost_func = static_cast<const Item_json_func *>(func->args[0]->real_item());
  } else {
    innermost_func = static_cast<const Item_json_func *>(func);
  }
  
  // Find the index of the child containing the json path argument
  size_t path_idx;
  if (innermost_func->func_name() == std::string("json_extract")) path_idx = 1;
  else if (innermost_func->func_name() == std::string("json_value")) path_idx = 1;
  else if (innermost_func->func_name() == std::string("json_unquote")) path_idx = 0;
  else return true;

  *json_path_arg = innermost_func->args[path_idx]->real_item();
  return false;
}

// Sets selectivity to some value probably within the range [0, 1].
// Does not take into account the total null_values fraction for the column.
// Does take into account the null_values fraction for the given key path.
// So if the column is 25% null values, and the key_path is present in 50% of documents
// and leads to null in 20% of cases, then the value 0.5 * (1 - 0.2) = 0.4 will be returned.
// The caller will have to multiply with the column's null values fraction to get the final 
// selectivity of (1 - 0.25) * 0.4 = 0.3.
bool Json_flex::get_selectivity(Item_func *func, Item **comparands, size_t comparand_count, enum_operator op, double *selectivity) const {
  // Check comparands and comparand count
  if (comparand_count > 1) {
    for (size_t i = 1; i < comparand_count; i++) {
      assert(comparands[i-1]->type() == comparands[i]->type());
    }
  }
  if (comparand_count > 1) {
    assert(op == enum_operator::BETWEEN 
        || op == enum_operator::NOT_BETWEEN
        || op == enum_operator::IN_LIST 
        || op == enum_operator::NOT_IN_LIST);
  }
  if (comparand_count == 0) {
    assert(op == enum_operator::IS_NULL 
      || op == enum_operator::IS_NOT_NULL);
  }
  


  // Record whether json_unquote is called. It's the only wrapper function currently supported.
  // Its absence means we don't have information about the expected type of the path terminal
  // TODO: Use func->functype() and Item_func::Functype enum instead. The enum seems to currently be missing a few of the functions we're checking against
  bool raw_value_returned = func->func_name() == std::string("json_unquote")
                         || func->func_name() == std::string("json_value");
  // TODO: Support for JSON_MEMBEROF and JSON_CONTAINS
  
  
  // Build query path
  Item *json_path_arg;
  get_json_func_path_item(func, &json_path_arg);
  std::string path_builder("");
  if (
    auto qstring_example_comparand = comparand_count > 0 ? comparands[0] : nullptr;
    build_histogram_query_string(json_path_arg, qstring_example_comparand, 
                                 raw_value_returned, path_builder)
  ) return true;
  const String arg_path = String(path_builder.c_str(), path_builder.length(), m_charset);


  if (comparand_count == 0) {
    // Checking for JSON null is kinda shitty. 
    // If I understand things correctly, this is how things work:
    // Using IS NULL and IS NOT NULL is really checking EXISTS and NOT EXISTS (for the given path)
    // Using IS NULL with JSON_VALUE basically combines NOT EXISTS and actually checking for a JSON null value
      // Using IS NOT NULL with JSON_VALUE returns what you would expect
    // To check for only JSON null, you have to do JSON_TYPE(JSON_EXTRACT(...)) = 'NULL'
    // Alternatively, one can use JSON_VALUE with some other default than NULL
    // Catching these two cases here won't be fun at all.

    // IS NULL checks whether the path exists and leads to a null value
    // IS NOT NULL checks whether the path exists and does not lead to a null value

    if (func->func_name() == std::string("json_value")) {
      // TODO: Check for default value not being null
      // if (func->arg_count >= 4 && func->children[3]...)
      switch (op) {
        case enum_operator::IS_NULL:
          *selectivity = 1 - get_not_eq_null_selectivity(arg_path);
          break;
        case enum_operator::IS_NOT_NULL:
          *selectivity = get_not_eq_null_selectivity(arg_path);
          break;
        default:
          assert(false);
          return true;
      }
    } else {
      switch (op) {
        case enum_operator::IS_NULL:
          *selectivity = 1 - get_exists_selectivity(arg_path);
          break;
        case enum_operator::IS_NOT_NULL:
          *selectivity = get_exists_selectivity(arg_path);
          break;
        default:
          assert(false);
          return true;
      }
    }

  // If json_unquote was called, and the comparand is a const, 
  // then we know that we have an actual value that we can lookup in specifically
  // in the histogram data. Otherwise, we can only look up the generated query string.
  } else if (raw_value_returned && comparands[0]->const_item()) {
    // Assume that if one of the items are const, then all are. This may not actually be the case (can you do "t1.col1 in (1, 2, t2.col2)"?).
    if (comparand_count > 1) {
      *selectivity = multi_val_dispatch(this, arg_path, op, comparands, comparand_count);
    } else {
      auto comparand = comparands[0];
      switch(comparand->type()) {
        // TODO: Do we differentiate between doubles and floats??
        case Item::Type::INT_ITEM: {
          // const Name_string *const name = &(comparand->item_name);
          // if (name->is_set() && (name->eq("FALSE") || name->eq("TRUE"))) {
          //   *selectivity = selectivity_getter_dispatch(this, arg_path, op, comparand->val_bool());
          // } else {
          //   *selectivity = selectivity_getter_dispatch(this, arg_path, op, comparand->val_int());
          // }
          *selectivity = selectivity_getter_dispatch(this, arg_path, op, comparand->val_int());
          break;
        }
        case Item::Type::REAL_ITEM: {
          *selectivity = selectivity_getter_dispatch(this, arg_path, op, comparand->val_real());
          break;
        }
        case Item::Type::STRING_ITEM: {
          StringBuffer<MAX_FIELD_WIDTH> str_buf(comparand->collation.collation);
          const String *str_val = comparand->val_str(&str_buf);
          // Compare truncated version of string, just in case something ridiculously longs was passed
          // In the future: all strings in histograms (excl. keypaths) will have a maximum length,
          // and comparing beyond that length will not be allowed
          // const String truncated = str_val->substr(0, HISTOGRAM_MAX_COMPARE_LENGTH);
          // *selectivity = selectivity_getter_dispatch<String>(this, arg_path, op, truncated);
          *selectivity = selectivity_getter_dispatch<String>(this, arg_path, op, *str_val);
          break;
        }
        case Item::Type::NULL_ITEM: {
          assert(false);
          return true;
        }
        case Item::Type::FUNC_ITEM: {
          if (comparand->val_int() == 1 || comparand->val_int() == 0) {
            *selectivity = selectivity_getter_dispatch(this, arg_path, op, comparand->val_bool());
            break;
          } else {
            assert(false);
            return true;
          }
        }
        default: {
          // TODO: Remove asserts here. We shouldn't actually crash on unsupported data types
          assert(false);
          return true;
        }
      }
    }
  } else {
    *selectivity = selectivity_getter_dispatch(this, arg_path, op);
  }

  return false;  
}

// Separators used when building the histogram query string
const std::string TYPE_SEP = "_";
const std::string KEY_SEP = ".";

size_t Json_flex::get_ndv(const Item_func *func) const {
  // We don't want to deal with 
  bool comparison_is_unquoted = func->func_name() == std::string("json_unquote")
                                || func->func_name() == std::string("json_value");
  if (!comparison_is_unquoted) return -1;

  // If int and float get separate suffixes, they must be added here as well
  size_t total_ndv = 0;
  for (const std::string suffix : {"num", "bool", "str"})
  {
    // This is not exactly the most efficient way of doing this. But it's very simple.

    // Build query path
    Item *json_path_arg;
    get_json_func_path_item(func, &json_path_arg);
    std::string path_builder("");
    if (
      build_histogram_query_string(json_path_arg, nullptr, false, path_builder)
    ) {
      assert(false);
      return -1;
    }
    path_builder.append(TYPE_SEP);
    path_builder.append(suffix);
    const String arg_path = String(path_builder.c_str(), path_builder.length(), m_charset);

    if (auto bucketOpt = find_bucket(arg_path)) {
      const JsonBucket *bucket = *bucketOpt;
      if (bucket->ndv) {
        total_ndv += (*bucket->ndv);
      }
    }
  }

  return total_ndv > 0 ? total_ndv : -1;
}

bool Json_flex::build_histogram_query_string(Item *json_path_arg, Item *comparand, bool arg_type_certain, std::string &builder) {
  // Copy string value in function argument  
  StringBuffer<MAX_FIELD_WIDTH> str_buf(json_path_arg->collation.collation);
  std::string str = to_string(*json_path_arg->val_str(&str_buf));

  // Parse the argument query string to build a string to query the histogram with
  // Example query string: docs[0].history.edits[5].datetime
  auto iterator_start = str.begin() + 1; // Skip the '$'
  auto iterator_end = str.begin() + 1;

outer_loop:
  while (iterator_end != str.end()) {
    if (*iterator_end == '.') {
      iterator_end += 1;
      iterator_start += 1;
    }

    // Array keys are simple -- they always start with [
    if (*iterator_end == '[') {
      iterator_start += 1; // skip the bracket
      
      // go to the end of the bracket
      do {
        iterator_end += 1;
      } while (*iterator_end != ']');
      
      // Append type information to previous key, if one exists
      if (builder.size() > 0) {
        builder.append(TYPE_SEP);
        builder.append("arr");
        builder.append(KEY_SEP);
      }

      builder.append(std::string(iterator_start, iterator_end));
      iterator_end += 1; // move past the ']'
    } else {
      // differentiating between objects and terminals is not as simple.
      // We have to go forward until we find an object or array accessor
      // or until we reach the end of the string.
      while (*iterator_end != '.' && *iterator_end != '[') {
        iterator_end += 1;

        // We found the terminal
        if (iterator_end == str.end()) {
          builder.append(std::string(iterator_start, iterator_end));
          goto outer_loop;
        }
      }

      // Append key
      builder.append(std::string(iterator_start, iterator_end));

      // If we found a dot, we know that the key is object type
      if (*iterator_end == '.') {
        builder.append(TYPE_SEP);
        builder.append("obj");
        builder.append(KEY_SEP);
      }
    }
    // if (iterator_end != str.end()) builder.append(KEY_SEP);
    iterator_start = iterator_end;
  }


  // If the JSON_VALUE is not called (i.e., -> is used instead of ->>), we can't use the type of of the comparand
  // and will have to lookup the key path for all terminal types. 
  // This check will also skip IS_NULL type queries. Null values do not get a suffix anyway.
  if (comparand && arg_type_certain) {
    switch(comparand->type()) {
      // TODO: Do we differentiate between doubles and floats??
      case Item::Type::INT_ITEM: {
        // Apparently, bool items are not passed as INT?
        // const Name_string *const name = &(comparand->item_name);
        // if (name && name->is_set() && (name->eq("FALSE") || name->eq("TRUE"))) {
        //   builder.append("_bool");
        // } else {
        //   builder.append("_num");
        // }
        builder.append("_num");
        break;
      }
      case Item::Type::REAL_ITEM: {
        builder.append("_num");
        break;
      }
      case Item::Type::STRING_ITEM: {
        builder.append("_str");
        break;
      }
      // TODO: Try to find a better way to identify bools. This is really ugly.
      case Item::Type::FUNC_ITEM: {

        // TODO: Fix this. The stuff below shouldn't be commented out, but I need to do so to make this work with 
        // the get_ndv() function, because I can't figure out how to set the value of an Item_func.
        builder.append("_bool");
        break;
        // if (comparand->val_int() == 1 || comparand->val_int() == 0) {
        // } else {
        //   assert(false);
        //   return true;
        // }
      }
      default: {
        assert(false);
        return true;
      }
    }
  } 

  return false;
}

std::optional<const JsonBucket *> Json_flex::find_bucket(const String &path) const {
  for (const JsonBucket *bucket = m_buckets.begin(); bucket != m_buckets.end(); bucket++) {
    if (stringcmp(&path, &bucket->key_path) == 0) {
      return bucket;
    }
  }
  return std::nullopt;
}

// ughh
template<>
Json_flex::lookup_result Json_flex::lookup_bucket(const String &path, const longlong cmp_val) const;


template<>
Json_flex::lookup_result Json_flex::lookup_bucket(const String &path, const double cmp_val) const {
  if (auto bucketOpt = find_bucket(path)) {
    const JsonBucket *bucket = *bucketOpt;
    double base_freq = bucket->frequency * (1.0 - bucket->null_values);

    if (bucket->min_val && bucket->max_val) {
      if ((*bucket->min_val)._float > cmp_val) {
        return lookup_result{0, 0, base_freq};
      } 
      if ((*bucket->max_val)._float < cmp_val) {
        return lookup_result{0, base_freq, 0};
      }
    }

    // If lookup value is a float but the bucket is actually full of integers,
    // and lookup value is a valid integer. Then we can do an integer lookup in the 
    // bucket instead
    // TODO: Should probably check that cmp_val is inside the range of a longlong
    if (bucket->values_type == BucketValueType::INT && std::ceil(cmp_val) == cmp_val) {
      return lookup_bucket(path, (longlong) cmp_val);
    }

    // Lookup cmp_val in histogram
    // assumes histogram buckets are sorted in ascending order
    if (bucket->histogram) {
      assert(bucket->values_type == BucketValueType::FLOAT);
      auto histogram = static_cast<JsonGram<double> *>(bucket->histogram);

      double cumulative = 0;
      if (histogram->buckets_type == JFlexHistType::SINGLETON) {
        for (const auto &jg_buck : histogram->m_buckets.single_bucks) {
          if (jg_buck.value == cmp_val) {
            return lookup_result{
              base_freq * jg_buck.frequency, 
              cumulative * base_freq, 
              (1 - (cumulative + jg_buck.frequency)) * base_freq
            };
          } else if (jg_buck.value > cmp_val) {
            // If buck_str > cmp_val, we should stop early.
            // No vals eq the cmp_val. 
            // As many less than this value as usual.
            // The current bucket value is greater than cmp_val, so we don't add the current bucket's frequency to the greater than field
            return lookup_result{
              0, 
              (cumulative) * base_freq, 
              (1 - cumulative) * base_freq
            };
          }
          cumulative += jg_buck.frequency;
        }
        // Should not be reachable?
        assert(false);
      } else {
        for (const auto &jg_buck : histogram->m_buckets.equi_bucks) {
          if (jg_buck.upper_bound >= cmp_val) {
            return lookup_result{
              (base_freq * jg_buck.frequency) / jg_buck.ndv,
              (cumulative) * base_freq,
              (1 - cumulative) * base_freq
            };
          }
          cumulative += jg_buck.frequency;
        }
        assert(false);
      }
    }

    // 
    if (bucket->ndv) {
      return lookup_result{base_freq / (*bucket->ndv), base_freq * 0.3, base_freq * 0.3};
    }

    return lookup_result{base_freq * 0.1, base_freq * 0.3, base_freq * 0.3};
  }
  return lookup_result{min_frequency * 0.1, min_frequency * 0.3, min_frequency * 0.3};
}

template<>
Json_flex::lookup_result Json_flex::lookup_bucket(const String &path, String cmp_val) const {
  if (auto bucketOpt = find_bucket(path)) {
    const JsonBucket *bucket = *bucketOpt;
    double base_freq = bucket->frequency * (1.0 - bucket->null_values);

    // Check if cmp_val out of range of bucket values
    if (bucket->min_val && bucket->max_val) {
      String min_string = (*bucket->min_val)._string.to_string();
      String max_string = (*bucket->max_val)._string.to_string();
      if (stringcmp(&min_string, &cmp_val) > 0) {
        return lookup_result{0, 0, base_freq};
      }
      if (stringcmp(&max_string, &cmp_val) < 0) {
        return lookup_result{0, base_freq, 0};
      }
    }

    if (bucket->histogram) {
      assert(bucket->values_type == BucketValueType::STRING);
      auto histogram = static_cast<JsonGram<BucketString> *>(bucket->histogram);

      double cumulative = 0;
      if (histogram->buckets_type == JFlexHistType::SINGLETON) {
        // Singleton histograms for floats is pretty sussy, but ok
        for (const auto &jg_buck : histogram->m_buckets.single_bucks) {
          String buck_str = jg_buck.value.to_string();
          auto cmp_result = stringcmp(&buck_str, &cmp_val);
          if (cmp_result == 0) {
            return lookup_result{
              base_freq * jg_buck.frequency, 
              cumulative * base_freq, 
              (1 - (cumulative + jg_buck.frequency)) * base_freq
            };
          } else if (cmp_result > 0) {
            // If buck_str > cmp_val, stop early.
            return lookup_result{
              histogram->rest_mean_frequency.value_or(0) * base_freq,
              (cumulative) * base_freq, 
              (1 - cumulative) * base_freq
            };
          }
          cumulative += jg_buck.frequency;
        }
        return lookup_result{
          histogram->rest_mean_frequency.value_or(0) * base_freq, 
          base_freq, 
          0
        };
      } else {
        assert(false); // No support for equi-height string histograms for now
      }
    }

    // 
    if (bucket->ndv) {
      return lookup_result{base_freq / (*bucket->ndv), base_freq * 0.3, base_freq * 0.3};
    }

    return lookup_result{base_freq * 0.1, base_freq * 0.3, base_freq * 0.3};
  }

  return lookup_result{min_frequency * 0.1, min_frequency * 0.3, min_frequency * 0.3};
}

// Returns {eq_estimate, 0, 0}, because can you even do gt/lt for boolean values??
template<>
Json_flex::lookup_result Json_flex::lookup_bucket(const String &path, const bool cmp_val) const {
  if (auto bucketOpt = find_bucket(path)) {
    const JsonBucket *bucket = *bucketOpt;
    double base_freq = bucket->frequency * (1.0 - bucket->null_values);

    if (bucket->min_val && bucket->max_val) {
      if ((*bucket->min_val)._bool == (*bucket->max_val)._bool) {
        return {
          (*bucket->min_val)._bool == cmp_val ? base_freq : 0, 
          0, 0
        };
      }
    }

    // Lookup cmp_val in histogram
    // assumes histogram buckets are sorted in ascending order
    if (bucket->histogram) {
      auto histogram = static_cast<JsonGram<bool> *>(bucket->histogram);

      assert(histogram->buckets_type == JFlexHistType::SINGLETON); // Say no to equi-height histograms for boolean values

      if (histogram->m_buckets.single_bucks.size() >= 1) {
        auto first = histogram->m_buckets.single_bucks[0];
        auto mult = first.value == cmp_val ? first.frequency : 1 - first.frequency;
        return lookup_result{mult * base_freq, 0, 0}; // Ignore LT and GT
      }
    }

    return lookup_result{base_freq * 0.5, base_freq * 0.5, base_freq * 0.5};
  }

  // If bucket can't be found, return global minimum frequency
  return {min_frequency * 0.5, min_frequency * 0.5, min_frequency * 0.5};
}

template<>
Json_flex::lookup_result Json_flex::lookup_bucket(const String &path, const longlong cmp_val) const {
  if (auto bucketOpt = find_bucket(path)) {
    const JsonBucket *bucket = *bucketOpt;
    double base_freq = bucket->frequency * (1.0 - bucket->null_values);

    // If float and int have the same type identifier (key path type suffix), then it
    // is possible that an integer comparand was used for a key path which actually holds float values
    // In this case, we convert the comparand to float and lookup that value instead:
    if (bucket->values_type == BucketValueType::FLOAT) {
      return lookup_bucket(path, (double) cmp_val);
    }

    // Check if cmp_val out of range of bucket values
    if (bucket->min_val && bucket->max_val) {
      if ((*bucket->min_val)._int > cmp_val) {
        return lookup_result{0, 0, base_freq};
      } 
      if ((*bucket->max_val)._int < cmp_val) {
        return lookup_result{0, base_freq, 0};
      }
    }

    // Lookup cmp_val in histogram
    // assumes histogram buckets are sorted in ascending order
    if (bucket->histogram) {
      assert(bucket->values_type == BucketValueType::INT);
      auto histogram = static_cast<JsonGram<longlong> *>(bucket->histogram);

      double cumulative = 0;
      if (histogram->buckets_type == JFlexHistType::SINGLETON) {
        for (const auto &jg_buck : histogram->m_buckets.single_bucks) {
          if (jg_buck.value == cmp_val) {
            return lookup_result{
              base_freq * jg_buck.frequency, 
              cumulative * base_freq, 
              (1 - (cumulative + jg_buck.frequency)) * base_freq
            };
          } else if (jg_buck.value > cmp_val) {
            return lookup_result{
              0, 
              (cumulative) * base_freq, 
              (1 - cumulative) * base_freq
            };
          }
          cumulative += jg_buck.frequency;
        }
      } else {
        for (const auto &jg_buck : histogram->m_buckets.equi_bucks) {
          if (jg_buck.upper_bound >= cmp_val) {
            return lookup_result{
              (base_freq * jg_buck.frequency) / jg_buck.ndv,
              (cumulative) * base_freq,
              (1 - cumulative) * base_freq
            };
          }
          cumulative += jg_buck.frequency;
        }
        // Should not be reachable, as final upper_bound < cmp_val should mean that 
        // cmp_val > max_val, which should have been caught above
        assert(false);
      }
    }

    // 
    if (bucket->ndv) {
      return lookup_result{base_freq / (*bucket->ndv), base_freq * 0.3, base_freq * 0.3};
    }

    return lookup_result{base_freq * 0.1, base_freq * 0.3, base_freq * 0.3};
  }

  // If bucket can't be found, return global minimum frequency
  return {min_frequency * 0.1, min_frequency * 0.3, min_frequency * 0.3};
}

Json_flex::lookup_result Json_flex::lookup_bucket(const String &path) const {
  if (auto bucketOpt = find_bucket(path)) {
    const JsonBucket *bucket = *bucketOpt;
    double base_freq = bucket->frequency * (1.0 - bucket->null_values);
    
    if (bucket->ndv) {
      return lookup_result{base_freq / (*bucket->ndv), base_freq * 0.3, base_freq * 0.3};
    }

    return lookup_result{base_freq * 0.1, base_freq * 0.3, base_freq * 0.3};
  }
  return lookup_result{min_frequency * 0.1, min_frequency * 0.3, min_frequency * 0.3};
}

template<typename T>
double Json_flex::get_not_equal_to_selectivity(const String &path, T cmp_val) const {
  if (auto bucketOpt = find_bucket(path)) {
    double eq_freq = lookup_bucket(path, cmp_val).eq_frequency;
    auto bucket = *bucketOpt;
    return (bucket->frequency * (1 - bucket->null_values)) - eq_freq;
  }
  return min_frequency * 0.9;
}

template<typename T>
double Json_flex::get_equal_to_selectivity(const String &path, T cmp_val) const {
  if (find_bucket(path)) {
    return lookup_bucket(path, cmp_val).eq_frequency;
  } else {
    // If we can't find a bucket for the given primitive path, we can try 
    // to look for a bucket without the type suffix. It will have less information
    // but it will still be better than nothing
    
    // This assumes that the functions which take cmp_val always take a path ending in a type suffix,
    // which should hold true.
    const String type_separator(TYPE_SEP.c_str(), TYPE_SEP.length(), m_charset);
    const int sep_offset = path.strrstr(type_separator, path.length());
    const String without_suffix = path.substr(0, sep_offset);
    return lookup_bucket(without_suffix).eq_frequency;
  }
}

template<typename T>
double Json_flex::get_less_than_selectivity(const String &path, T cmp_val) const {
  return lookup_bucket(path, cmp_val).lt_frequency;
}
template<typename T>
double Json_flex::get_greater_than_selectivity(const String &path, T cmp_val) const {
  return lookup_bucket(path, cmp_val).gt_frequency;
}

double Json_flex::get_not_equal_to_selectivity(const String &path) const {
  if (auto bucketOpt = find_bucket(path)) {
    double eq_freq = lookup_bucket(path).eq_frequency;
    auto bucket = *bucketOpt;
    return (bucket->frequency * (1 - bucket->null_values)) - eq_freq;
  }
  return min_frequency * 0.9;
}

double Json_flex::get_equal_to_selectivity(const String &path) const {
  return lookup_bucket(path).eq_frequency;
}

double Json_flex::get_less_than_selectivity(const String &path) const {
  return lookup_bucket(path).lt_frequency;
}

double Json_flex::get_greater_than_selectivity(const String &path) const {
  return lookup_bucket(path).gt_frequency;
}
double Json_flex::get_not_eq_null_selectivity(const String &path) const {
  if (auto bucketOpt = find_bucket(path)) {
    auto bucket = *bucketOpt;
    return bucket->frequency * (1 - bucket->null_values);
  }
  // TODO: What does MySQL use elsewhere for =/<> NULL estimation?
  return min_frequency * 0.8; // Assume 20% of values are null
}
double Json_flex::get_eq_null_selectivity(const String &path) const {
  if (auto bucketOpt = find_bucket(path)) {
    auto bucket = *bucketOpt;
    return bucket->frequency * bucket->null_values;
  }
  return min_frequency * 0.2; // Assume 20% of values are null
}
double Json_flex::get_exists_selectivity(const String &path) const {
  if (auto bucketOpt = find_bucket(path)) {
    return (*bucketOpt)->frequency;
  }
  return min_frequency;
}

}  // namespace histograms
