#ifndef HISTOGRAMS_JSON_FLEX_INCLUDED
#define HISTOGRAMS_JSON_FLEX_INCLUDED


#include <stddef.h>
#include <string>  // std::string

#include "my_inttypes.h"
#include "mysql_time.h"
#include "sql/histograms/histogram.h"  // Histogram, Histogram_comparator,
#include "sql/histograms/value_map_type.h"
#include "sql/mem_root_allocator.h"
#include "sql/mem_root_array.h"
#include "sql/my_decimal.h"
#include "sql_string.h"
#include "sql/item.h"
#include "sql/item_func.h"

class Json_array;
class Json_object;
struct MEM_ROOT;

namespace histograms {

/**
  Json_flex histogram.

  Json_flex histograms do not have a public constructor, but are instead created
  through the factory method Json_flex::create() and returned by pointer.
  This is done to ensure that we can return nullptr in case memory allocations
  carried out during construction fail.

  Likewise, the Json_flex class does not have a public copy constructor, but
  instead implements a clone() method that returns nullptr in case of failure.
*/
enum class BucketValueType {
  UNKNOWN,
  INT,
  FLOAT,
  STRING,
  BOOL,
};


// Basically a very crude copy of the MySQL String class, without constructors and destructors 
// so that we can put it in our union without to resort to various workarounds.
struct BucketString {
  char *m_ptr;
  size_t m_length;
  const CHARSET_INFO *m_charset;

  String to_string() const {
    return String(m_ptr, m_length, m_charset);
  }
};

union json_primitive {
  double _float;
  longlong _int; // May lead to trouble when/if a longlong can't accommodate the same range as a json int (double) can.
  bool _bool;
  BucketString _string;
};
typedef std::optional<json_primitive> maybe_primitive;


// NOTE: When adding new members, take care to handle:
// * Copy constructor
// * histogram_to_json
// * json_to_histogram
// * create_json_bucket
// * (if a new type was added): Add a new add_value_json_bucket for the type
#define JSON_BUCKET_TOTAL_MEMBER_COUNT 6
#define JSON_BUCKET_OPTIONAL_MEMBER_COUNT 3
struct JsonBucket {
  const String key_path;
  const double frequency;
  const double null_values; // Frequency with which the key_path leads to null (distinct from not being present)

  // Optional members
  const maybe_primitive min_val;
  const maybe_primitive max_val;
  const std::optional<longlong> ndv;

  // Assigned at creation.
  const BucketValueType values_type;

  JsonBucket(String key_path, double frequency, double null_values)
      : key_path(key_path), frequency(frequency),
        null_values(null_values),
        min_val(std::nullopt), max_val(std::nullopt),
        ndv(std::nullopt),
        values_type(BucketValueType::UNKNOWN){}

  JsonBucket(String key_path, double frequency, double null_values,
             maybe_primitive min_val, maybe_primitive max_val, std::optional<longlong> ndv, BucketValueType values_type)
      : key_path(key_path), frequency(frequency),
        null_values(null_values),
        min_val(min_val), max_val(max_val),
        ndv(ndv),
        values_type(values_type){}

    // ~JsonBucket() {
    //   if (values_type == BucketValueType::STRING) {
    //     if (min_val) my_free((*min_val)._string.m_ptr);
    //     if (max_val) my_free((*max_val)._string.m_ptr);
    //   }
    // }
};

class Json_flex : public Histogram {
 public:
  /**
    Json_flex histogram factory method.

    Attempts to allocate and initialize a json_flex histogram on the supplied
    mem_root. This will not build the histogram, but only set its properties.
    If the attempt to allocate the histogram fails or if an error occurs during
    construction we return nullptr.

    @param mem_root the mem_root where the histogram contents will be allocated
    @param db_name  name of the database this histogram represents
    @param tbl_name name of the table this histogram represents
    @param col_name name of the column this histogram represents

    @return A pointer to a Json_flex histogram on success. Returns nullptr on
    error.
  */
  static Json_flex *create(MEM_ROOT *mem_root, const std::string &db_name,
                              const std::string &tbl_name,
                              const std::string &col_name);

  /**
    Make a clone of this histogram on a MEM_ROOT.

    @param mem_root the MEM_ROOT to allocate the new histogram contents on.

    @return a copy of the histogram allocated on the provided MEM_ROOT.
  */
  Histogram *clone(MEM_ROOT *mem_root) const override;

  Json_flex(const Json_flex &other) = delete;

  /**
    Convert this histogram to a JSON object.

    This function will take the contents of the current histogram and put
    it in the output parameter "json_object".

    @param[in,out] json_object output where the histogram is to be stored. The
                   caller is responsible for allocating/deallocating the JSON
                   object

    @return        true on error, false otherwise
  */
  bool histogram_to_json(Json_object *json_object) const override;

  /**
    @return json_primitive of values/buckets in this histogram
  */
  size_t get_num_buckets() const override { return m_buckets.size(); }

  /**
    Get the estimated json_primitive of distinct non-NULL values.
    @return json_primitive of distinct non-NULL values

    TODO(christiani): If the histogram is based on sampling, then this estimate
    is potentially off by a factor 1/sampling_rate. It should be adjusted to an
    actual estimate if we are going to use it.
  */
  size_t get_num_distinct_values() const override { return get_num_buckets(); }

  /**
    Returns the histogram type as a readable string.

    @return a readable string representation of the histogram type
  */
  std::string histogram_type_to_str() const override;

  /**
  
    @param func        The Item_func of the json function: JSON_EXTRACT, JSON_CONTAINS, ...
    @param comparand   The argument to the function -- usually the compare value
    @param op          The operator type
    @param selectivity Double into which the found selectivity will be placed

    TODO: consider making func have the Item_json_func type instead
  */
  bool get_selectivity(Item_func *func, Item *comparand, enum_operator op, double *selectivity) const;


  double get_equal_to_selectivity(const String &path) const;
  double get_less_than_selectivity(const String &path) const;
  double get_greater_than_selectivity(const String &path) const;
  
  template<typename T>
  double get_equal_to_selectivity(const String &path, const T value) const;
  template<typename T>
  double get_less_than_selectivity(const String &path, const T value) const;
  template<typename T>
  double get_greater_than_selectivity(const String &path, const T value) const;

  // HELPER FUNCTIONS:

  std::optional<const JsonBucket *> find_bucket(const String &path) const;
  double lookup_bucket(const String &path) const;
  template<typename T>
  double lookup_bucket(const String &path, const T cmp_val) const;
  
 protected:
  /**
    Populate this histogram with contents from a JSON object.

    @param json_object  a JSON object that represents an Json_flex histogram
    @param context      error context for validation

    @return true on error, false otherwise.
   */
  bool json_to_histogram(const Json_object &json_object,
                         Error_context *context) override;


 private:
  /**
    Build the string used to query the histogram for selectivity of the given operand
  */
  static bool build_histogram_query_string(Item *json_path_arg, Item *comparand, bool arg_type_certain, std::string &builder);

  /// Expected length of a json representation of the Json_flex bucket
  static constexpr size_t json_bucket_total_member_count() { return JSON_BUCKET_TOTAL_MEMBER_COUNT; }
  static constexpr size_t json_bucket_optional_member_count() { return JSON_BUCKET_OPTIONAL_MEMBER_COUNT; }
  /// String representation of the histogram type JSON_FLEX.
  static constexpr const char *json_flex_str() { return "json-flex"; }
  /// Minimum frequency encountered in the bucket. Any value not found should have lower frequency
  double min_frequency = 1.0; // Make easily recognizable for now. TODO: Set to 1.0 

  /**
    Json_flex constructor.

    This will not build the histogram, but only set its properties.

    @param mem_root  the mem_root where the histogram contents will be allocated
    @param db_name   name of the database this histogram represents
    @param tbl_name  name of the table this histogram represents
    @param col_name  name of the column this histogram represents
    @param[out] error is set to true if an error occurs
  */
  Json_flex(MEM_ROOT *mem_root, const std::string &db_name,
            const std::string &tbl_name, const std::string &col_name,
            bool *error);

  /**
    Json_flex copy-constructor

    This will take a copy of the histogram and all of its contents on the
    provided MEM_ROOT.

    @param mem_root   the MEM_ROOT to allocate the new histogram on.
    @param other      the histogram to take a copy of
    @param[out] error is set to true if an error occurs
  */
  Json_flex(MEM_ROOT *mem_root, const Json_flex &other, bool *error);

  /**
    Add value to a JSON bucket

    This function adds the value to the supplied JSON array.

    @param      value       the value to add
    @param[out] json_bucket a JSON array where the bucket data is to be stored

    @return     true on error, false otherwise
  */
  static bool add_value_json_bucket(const BucketString &value, Json_array *json_bucket);
  static bool add_value_json_bucket(const String &value, Json_array *json_bucket);
  static bool add_value_json_bucket(const double &value, Json_array *json_bucket);
  static bool add_value_json_bucket(const longlong &value, Json_array *json_bucket);
  static bool add_value_json_bucket(const bool &value, Json_array *json_bucket);

  /**
    Convert one bucket to a JSON object.

    @param      bucket      the histogram bucket to convert
    @param[out] json_bucket a JSON array where the bucket data is to be stored

    @return     true on error, false otherwise
  */
  static bool create_json_bucket(const JsonBucket &bucket,
                                 Json_array *json_bucket);

  /// The buckets for this histogram
  Mem_root_array<JsonBucket> m_buckets;
};

}  // namespace histograms

#undef JSON_BUCKET_TOTAL_MEMBER_COUNT
#undef JSON_BUCKET_OPTIONAL_MEMBER_COUNT
#endif
