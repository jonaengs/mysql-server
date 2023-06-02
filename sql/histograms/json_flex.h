#ifndef HISTOGRAMS_JSON_FLEX_INCLUDED
#define HISTOGRAMS_JSON_FLEX_INCLUDED


#include <stddef.h>
#include <string>  // std::string
#include <any>

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

class Json_flex;

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
enum class JFlexHistType {
  SINGLETON, EQUI_HEIGHT,
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

  static BucketString from_string(String &str) {
    return BucketString{str.ptr(), str.length(), str.charset()};
  }

  /**
   It is up to the caller to check for OOM error by checking the duplicate's m_ptr
  */
  static BucketString dupe(MEM_ROOT *mem_root, BucketString &to_be_duped) {
    String s = to_be_duped.to_string();
    String duped = String(s.dup(mem_root), s.length(), s.charset());
    if (duped.ptr() == nullptr) assert(false);
    return BucketString::from_string(duped);
  }

  bool dupe(MEM_ROOT *mem_root, BucketString *into) const {
    String s = this->to_string();
    BucketString duped = BucketString{s.dup(mem_root), s.length(), s.charset()};
    if (duped.m_ptr == nullptr) {
      assert(false);
      return true;
    }
    *into = duped;
    return false;
  }
};


// TODO: Change from 'longlong' to int64_t
union json_primitive {
  double _float;
  longlong _int; // May lead to trouble when/if a longlong can't accommodate the same range as a json int (double) can.
  bool _bool;
  BucketString _string;
};
typedef std::optional<json_primitive> maybe_primitive;


// Histogram inside a Json_flex histogram's bucket
// Allowed types are the same as in json_primitive. Type is indicated by 
// Apologies for the terrible name
template<typename T> 
struct JsonGram {
  struct SingleBucket {
    T value;
    double frequency; // As a fraction of the total frequency of the key_path
  };
  struct EquiBucket {
    T upper_bound;
    double frequency;
    longlong ndv;
  };
  union Buckets {
    Mem_root_array<SingleBucket> single_bucks;
    Mem_root_array<EquiBucket> equi_bucks;
  };

  JFlexHistType buckets_type;
  Buckets m_buckets;
  // rest_mean_frequency is used in conjunction with Singleton buckets when there are more items than the singleton can hold,
  // but an equi-height cannot be used (basically, for strings).
  // It is useful for things like lists of enum strings, where some enums are super common, while 
  // a majority of enum values appears very infrequently. Additionally, queries are unlikely 
  // to match  against strings which do not appear in the data.
  // For example, in the twitter data set ("test" - 20k docs), the key-path source_str has ndv 37, but 
  // one of those values has a frequency of 90, while another has a frequency 8. The remaining strings have
  // a combined frequency of 2. In this case, we store those two most frequent strings in the singleton histogram
  // and then store the mean frequency of the remaining items: 0.02/35 = 0.0006.
  // This field holds the mean frequency of the items not included in the singleton histogram.
  std::optional<double> rest_mean_frequency = std::nullopt;

  // creation functions assume no OOM. Caller can check for null pointer
  static JsonGram<T>* create_singlegram(MEM_ROOT *mem_root);
  static JsonGram<T>* create_equigram(MEM_ROOT *mem_root);
  bool json_to_json_gram(const Json_array *buckets_array, Json_flex *parent, Error_context *context);
  bool populate_json_array(Json_array *buckets_array);
  JsonGram<T> *duplicate_onto(MEM_ROOT *mem_root);

  const char *get_bucket_type_str() {
    return (buckets_type == JFlexHistType::SINGLETON) ? 
      singlebucket_str() : equibucket_str();
  }

  JsonGram<T>* copy_struct(MEM_ROOT *mem_root) {
    return (buckets_type == JFlexHistType::SINGLETON) ? 
      create_singlegram(mem_root) : create_equigram(mem_root);
  }
  
  // JsonGram<T>* copy_struct(MEM_ROOT *mem_root, BucketValueType values_type) {
  //   if (buckets_type == JFlexHistType::SINGLETON) {
  //     switch (values_type) {
  //       case BucketValueType::INT: return std::any_cast<JsonGram<std::any> *>(JsonGram<longlong>::create_singlegram(mem_root));
  //       case BucketValueType::FLOAT: return std::any_cast<JsonGram<std::any> *>(JsonGram<double>::create_singlegram(mem_root));
  //       case BucketValueType::BOOL: return std::any_cast<JsonGram<std::any> *>(JsonGram<bool>::create_singlegram(mem_root));
  //       case BucketValueType::STRING: return std::any_cast<JsonGram<std::any> *>(JsonGram<BucketString>::create_singlegram(mem_root));
  //       default: assert(false);
  //     }
  //   } else {
  //     switch (values_type) {
  //       case BucketValueType::INT: return std::any_cast<JsonGram<std::any> *>(JsonGram<longlong>::create_equigram(mem_root));
  //       case BucketValueType::FLOAT: return std::any_cast<JsonGram<std::any> *>(JsonGram<double>::create_equigram(mem_root));
  //       case BucketValueType::BOOL: return std::any_cast<JsonGram<std::any> *>(JsonGram<bool>::create_equigram(mem_root));
  //       case BucketValueType::STRING: return std::any_cast<JsonGram<std::any> *>(JsonGram<BucketString>::create_equigram(mem_root));
  //       default: assert(false);
  //     }
  //   }
  // }

  static constexpr const char *singlebucket_str() { return "singleton"; }
  static constexpr const char *equibucket_str() { return "equi-height"; }
  static constexpr const char *type_str() { return "type"; }
  static constexpr const char *rest_frequency_string() { return "rest_frequency"; }
};

// union anygram {
//   JsonGram<double> *fgram;
//   JsonGram<longlong> *igram;
//   JsonGram<bool> *bgram; 
//   JsonGram<BucketString> *sgram;
// };


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
  void *histogram;  // ptr to a JsonGram or a nullptr

  // Assigned at creation.
  const BucketValueType values_type; // The type of the values contained in the bucket (min/max an in json_gram)

  JsonBucket(String key_path, double frequency, double null_values)
      : key_path(key_path), frequency(frequency),
        null_values(null_values),
        min_val(std::nullopt), max_val(std::nullopt),
        ndv(std::nullopt), histogram(nullptr),
        values_type(BucketValueType::UNKNOWN){}

  JsonBucket(String key_path, double frequency, double null_values,
             maybe_primitive min_val, maybe_primitive max_val, std::optional<longlong> ndv, 
             BucketValueType values_type, void *json_gram)
      : key_path(key_path), frequency(frequency),
        null_values(null_values),
        min_val(min_val), max_val(max_val),
        ndv(ndv), histogram(json_gram),
        values_type(values_type){}

    // ~JsonBucket() {
    //   if (values_type == BucketValueType::STRING) {
    //     if (min_val) my_free((*min_val)._string.m_ptr);
    //     if (max_val) my_free((*max_val)._string.m_ptr);
    //   }
    // }
};

class Json_flex : public Histogram {
  struct lookup_result {
    double eq_frequency;
    double lt_frequency;
    double gt_frequency;
  };

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
  
    @param func           The Item_func of the json function: JSON_EXTRACT, JSON_CONTAINS, ...
    @param comparands     The argument(s) to the function -- usually the compare value. Operations like BETWEEN and IN_LIST pass several comparands
    @param num_comparands The number of comparands -- the length of the array that comparands points to
    @param op             The operator type
    @param selectivity    Double into which the found selectivity will be placed

    TODO: consider making func have the Item_json_func type instead
  */
  bool get_selectivity(Item_func *func, Item **comparands, size_t num_comparands, enum_operator op, double *selectivity) const;

  double get_not_equal_to_selectivity(const String &path) const;
  double get_equal_to_selectivity(const String &path) const;
  double get_less_than_selectivity(const String &path) const;
  double get_greater_than_selectivity(const String &path) const;
  double get_not_eq_null_selectivity(const String &path) const;
  double get_eq_null_selectivity(const String &path) const;
  
  template<typename T>
  double get_not_equal_to_selectivity(const String &path, const T value) const;
  template<typename T>
  double get_equal_to_selectivity(const String &path, const T value) const;
  template<typename T>
  double get_less_than_selectivity(const String &path, const T value) const;
  template<typename T>
  double get_greater_than_selectivity(const String &path, const T value) const;

  // HELPER FUNCTIONS:

  template<typename T>
  lookup_result lookup_bucket(const String &path, const T cmp_val) const;
  lookup_result lookup_bucket(const String &path) const;
  std::optional<const JsonBucket *> find_bucket(const String &path) const;
  
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

// Make public so we can access from JsonGram. Only temporary until a better solution can be found
public:
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
private:
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
