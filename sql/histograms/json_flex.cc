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
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/item_json_func.h"

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
    char *string_data = bucket.key_path.dup(mem_root);
    if (string_data == nullptr) {
      *error = true;
      assert(false); /* purecov: deadcode */
      return;        // OOM
    }

    String string_dup(string_data, bucket.key_path.length(),
                      bucket.key_path.charset());

    JsonBucket copy(string_dup, bucket.frequency, bucket.null_values,
                   bucket.min_val.has_value() ? std::optional(*bucket.min_val) : std::nullopt, 
                   bucket.max_val.has_value() ? std::optional(*bucket.max_val) : std::nullopt
                  );

    if (bucket.min_val || bucket.max_val) {
      // If one of the optionals is included, the other should be as well
      assert(bucket.max_val);
      assert(copy.min_val);
      assert(copy.max_val);
      // Copied values should match
      assert((*copy.min_val)._int == (*bucket.min_val)._int);
      assert((*copy.max_val)._int == (*bucket.max_val)._int);
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
  if (bucket.min_val) {
    if (add_value_json_bucket((*bucket.min_val)._int, json_bucket))
      return true;

    if (add_value_json_bucket((*bucket.max_val)._int, json_bucket))
      return true;
  }
  
  return false;
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

std::string Json_flex::histogram_type_to_str() const {
  return json_flex_str();
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
    const size_t allowed_size_all = json_bucket_total_member_count();
    const size_t allowed_size_wo_opts = allowed_size_all - json_bucket_optional_member_count();
    // Either all or none of the optional values must be provided
    if (bucket->size() != allowed_size_all && bucket->size() != allowed_size_wo_opts) {
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
    

    // GET THIRD BUCKET ITEM: null_values
    const Json_dom *null_values_dom = (*bucket)[2];
    double null_values;
    if (extract_json_dom_value(null_values_dom, &null_values, context)) return true;


    std::optional<number> min_val_opt;
    std::optional<number> max_val_opt;
    if (bucket->size() == allowed_size_all) {
      // GET FOURTH BUCKET ITEM: min_val
      const Json_dom *min_val_dom = (*bucket)[3];
      const Json_dom *max_val_dom = (*bucket)[4];
      number min_val;
      number max_val;

      assert(min_val_dom->json_type() == max_val_dom->json_type());
      if (min_val_dom->json_type() == enum_json_type::J_DOUBLE) {
        if (extract_json_dom_value(min_val_dom, &(min_val._float), context)) return true;
        if (extract_json_dom_value(max_val_dom, &(max_val._float), context)) return true;
      } else if (min_val_dom->json_type() == enum_json_type::J_INT ||
                 min_val_dom->json_type() == enum_json_type::J_UINT) {
        if (extract_json_dom_value(min_val_dom, &min_val._int, context)) return true;
        if (extract_json_dom_value(max_val_dom, &max_val._int, context)) return true;
      }
      else {
        context->report_node(bucket_dom, Message::JSON_WRONG_ATTRIBUTE_TYPE);
        return true;
      }

      min_val_opt = min_val;
      max_val_opt = max_val;
      // TODO: look at final type string in the key path to figure out type of min and max value
    }
    

    // STORE BUCKET IN HISTOGRAM
    JsonBucket hist_bucket = JsonBucket(key_path, frequency, null_values, min_val_opt, max_val_opt);
    assert(m_buckets.capacity() > m_buckets.size());
    m_buckets.push_back(hist_bucket);
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

template<typename T>
double selectivity_getter_dispatch(const Json_flex *jflex, const String &arg_path, enum_operator op, T value) {
  switch(op) {
    case enum_operator::EQUALS_TO: {
      return jflex->get_equal_to_selectivity(arg_path, value);
    }
    case enum_operator::LESS_THAN: {
      return jflex->get_less_than_selectivity(arg_path, value);
    }
    case enum_operator::GREATER_THAN: {
      return jflex->get_greater_than_selectivity(arg_path, value);
    }
    default: assert(false);
  }
}

double selectivity_getter_dispatch(const Json_flex *jflex, const String &arg_path, enum_operator op) {
  switch(op) {
    case enum_operator::EQUALS_TO: {
      return jflex->get_equal_to_selectivity(arg_path);
    }
    case enum_operator::LESS_THAN: {
      return jflex->get_less_than_selectivity(arg_path);
    }
    case enum_operator::GREATER_THAN: {
      return jflex->get_greater_than_selectivity(arg_path);
    }
    default: assert(false);
  }
}

// TODO: Handle bools as well
bool Json_flex::get_selectivity(Item_func *func, Item *comparand, enum_operator op, double *selectivity) const {
  // Currently, we'll handle the JSON_EXTRACT function.
  // It takes a json_doc (simplifying assumption: a column), and a string path


  // Record whether json_unquote is called. It's the only wrapper function currently supported.
  // Its absence means we don't have information about the expected type of the path terminal.
  bool json_unquote_called = func->func_name() == std::string("json_unquote");
  
  // Find the innermost function in the (potentially) nested set of function calls.
  Item_json_func *innermost_func;
  if (json_unquote_called) {
    innermost_func = static_cast<Item_json_func *>(func->args[0]->real_item());
  } else {
    innermost_func = static_cast<Item_json_func *>(func);
  }
  
  // Find the index of the child containing the json path argument
  size_t path_idx;
  if (innermost_func->func_name() == std::string("json_extract")) path_idx = 1;
  else if (innermost_func->func_name() == std::string("json_unquote")) path_idx = 0;
  else return true;
  // TODO: Support for JSON_MEMBEROF and JSON_CONTAINS will require rewriting this first part


  // Build query path
  Item *json_path_arg = innermost_func->args[path_idx]->real_item();
  std::string path_builder("");
  if (
    build_histogram_query_string(json_path_arg, comparand, json_unquote_called, path_builder)
  ) return true;
  const String arg_path = String(path_builder.c_str(), path_builder.length(), m_charset);


  // If json_unquote was called, and the comparand is a const, 
  // then we know that we have an actual value that we can lookup in specifically
  // in the histogram data. Otherwise, we can only look up the generated query string.
  bool comparand_is_const = comparand->const_item();
  if (json_unquote_called && comparand_is_const) {
    switch(comparand->type()) {
      // TODO: Do we differentiate between doubles and floats??
      case Item::Type::INT_ITEM: {
        *selectivity = selectivity_getter_dispatch(this, arg_path, op, comparand->val_int());
        break;
      }
      case Item::Type::REAL_ITEM: {
        *selectivity = selectivity_getter_dispatch(this, arg_path, op, comparand->val_real());
        break;
      }
      case Item::Type::STRING_ITEM: {
        StringBuffer<MAX_FIELD_WIDTH> str_buf(comparand->collation.collation);
        const String *str = comparand->val_str(&str_buf);
        const String truncated = str->substr(0, HISTOGRAM_MAX_COMPARE_LENGTH);
        *selectivity = selectivity_getter_dispatch<const String&>(this, arg_path, op, truncated);
        break;
      }
      case Item::Type::NULL_ITEM: {
        // TODO: Handle = NULL
        // Will we have to handle >, =<, ... here?
        return true;
      }
      default: return true;
    }
  } else {
    *selectivity = selectivity_getter_dispatch(this, arg_path, op);
  }

  return false;  
}

// Separators used when building the histogram query string
const std::string TYPE_SEP = "_";
const std::string KEY_SEP = ".";

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

      // Append type information to previous key, if one exists
      if (builder.size() > 0) {
          builder.append(TYPE_SEP);
          builder.append("obj");
          builder.append(KEY_SEP);
      }
      builder.append(std::string(iterator_start, iterator_end));
    }
    // if (iterator_end != str.end()) builder.append(KEY_SEP);
    iterator_start = iterator_end;
  }


  // If the JSON_VALUE is not called (i.e., -> is used instead of ->>), we can't use the type of of the comparand
  // and will have to lookup the key path for all terminal types. 
  if (arg_type_certain) {
    // TODO: we can access the operands value here by using comparand->val_X(), where X is int, double, string
    switch(comparand->type()) {
      // TODO: Do we differentiate between doubles and floats??
      case Item::Type::INT_ITEM: {
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
      // TODO: Do we handle json_memberof/json_contains arguments here?
      default: return true;
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

template<>
double Json_flex::lookup_bucket(const String &path, const double cmp_val) const {
  if (auto bucketOpt = find_bucket(path)) {
    const JsonBucket *bucket = *bucketOpt;
    if (bucket->min_val && bucket->max_val) {
      if ((*bucket->min_val)._float > cmp_val || (*bucket->max_val)._float < cmp_val) {
        return 0.0;
      }
    }
    return bucket->frequency;
  }
  return min_frequency;
}

template<>
double Json_flex::lookup_bucket(const String &path, const longlong cmp_val) const {
  if (auto bucketOpt = find_bucket(path)) {
    const JsonBucket *bucket = *bucketOpt;
    if (bucket->min_val && bucket->max_val) {
      if ((*bucket->min_val)._int > cmp_val || (*bucket->max_val)._int < cmp_val) {
        return 0.0;
      }
    }
    return bucket->frequency * (1.0 - bucket->null_values);
  }
  return min_frequency;
}

// template<>
// double Json_flex::lookup_bucket(const String &path, const String &cmp_val) const {
//   auto _ = cmp_val; // stop unused param warning
//   return lookup_bucket(path);
// }

template<>
double Json_flex::lookup_bucket(const String &path, String cmp_val) const {
  auto _ = cmp_val; // stop unused param warning
  return lookup_bucket(path);
}

double Json_flex::lookup_bucket(const String &path) const {
  if (auto bucketOpt = find_bucket(path)) {
    const JsonBucket *bucket = *bucketOpt;
    return bucket->frequency * (1.0 - bucket->null_values);
  }
  return min_frequency;
}

template<typename T>
double Json_flex::get_equal_to_selectivity(const String &path, T cmp_val) const {
  return lookup_bucket(path, cmp_val);
}
template<typename T>
double Json_flex::get_less_than_selectivity(const String &path, T cmp_val) const {
  return lookup_bucket(path, cmp_val);
}
template<typename T>
double Json_flex::get_greater_than_selectivity(const String &path, T cmp_val) const {
  return lookup_bucket(path, cmp_val);
}


double Json_flex::get_equal_to_selectivity(const String &path) const {
  return lookup_bucket(path);
}

double Json_flex::get_less_than_selectivity(const String &path) const {
  return lookup_bucket(path);
}

double Json_flex::get_greater_than_selectivity(const String &path) const {
  return lookup_bucket(path);
}

}  // namespace histograms
