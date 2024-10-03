#include <cstring>
#include <db/Tuple.hpp>
#include <stdexcept>
#include <unordered_set>

using namespace db;

Tuple::Tuple(const std::vector<field_t> &fields) : fields(fields) {}

type_t Tuple::field_type(size_t i) const {
  const field_t &field = fields.at(i);
  if (std::holds_alternative<int>(field)) {
    return type_t::INT;
  }
  if (std::holds_alternative<double>(field)) {
    return type_t::DOUBLE;
  }
  if (std::holds_alternative<std::string>(field)) {
    return type_t::CHAR;
  }
  throw std::logic_error("Unknown field type");
}

size_t Tuple::size() const { return fields.size(); }

const field_t &Tuple::get_field(size_t i) const { return fields.at(i); }

TupleDesc::TupleDesc(const std::vector<type_t> &types, const std::vector<std::string> &names)
  // TODO pa2: add initializations if needed
{
  // TODO pa2: implement
  if (types.size() != names.size()) {
    throw std::logic_error("Mismatched types and names length.");
  }

  std::unordered_set<std::string> unique_names;
  for (const auto &name : names) {
    if (!unique_names.insert(name).second) {
      throw std::logic_error("Duplicate field name: " + name);
    }
  }

  // Store types and names
  this->types = types;
  this->names = names;
}

bool TupleDesc::compatible(const Tuple &tuple) const {
  // TODO pa2: implement
  if (tuple.size() != types.size()) {
    return false;
  }

  for (size_t i = 0; i < types.size(); ++i) {
    if (tuple.field_type(i) != types[i]) {
      return false;
    }
  }

  return true;
}

size_t TupleDesc::index_of(const std::string &name) const {
  // TODO pa2: implement
  for (size_t i = 0; i < names.size(); ++i) {
    if (names[i] == name) {
      return i;
    }
  }
  throw std::logic_error("Field name not found: " + name);
}

size_t TupleDesc::offset_of(const size_t &index) const {
  // TODO pa2: implement
  if (index >= types.size()) {
    throw std::out_of_range("Index out of range.");
  }

  size_t offset = 0;
  for (size_t i = 0; i < index; ++i) {
    switch (types[i]) {
      case type_t::INT:
        offset += INT_SIZE;
        break;
      case type_t::DOUBLE:
        offset += DOUBLE_SIZE;
        break;
      case type_t::CHAR:
        offset += CHAR_SIZE;
        break;
    }
  }
  return offset;
}

size_t TupleDesc::length() const {
  // TODO pa2: implement
  size_t totalLength = 0;
  for (const auto &type : types) {
    switch (type) {
      case type_t::INT:
        totalLength += INT_SIZE;
        break;
      case type_t::DOUBLE:
        totalLength += DOUBLE_SIZE;
        break;
      case type_t::CHAR:
        totalLength += CHAR_SIZE;
        break;
    }
  }
  return totalLength;
}

size_t TupleDesc::size() const {
  // TODO pa2: implement
  return types.size();
}

Tuple TupleDesc::deserialize(const uint8_t *data) const {
  // TODO pa2: implement
}

void TupleDesc::serialize(uint8_t *data, const Tuple &t) const {
  // TODO pa2: implement
  if (!compatible(t)) {
    throw std::logic_error("Tuple is not compatible with this TupleDesc.");
  }

  size_t offset = 0;
  for (size_t i = 0; i < t.size(); ++i) {
    const field_t &field = t.get_field(i);
    switch (types[i]) {
      case type_t::INT:
        std::memcpy(data + offset, &std::get<int>(field), INT_SIZE);
        offset += INT_SIZE;
        break;
      case type_t::DOUBLE:
        std::memcpy(data + offset, &std::get<double>(field), DOUBLE_SIZE);
        offset += DOUBLE_SIZE;
        break;
      case type_t::CHAR: {
        const std::string &str = std::get<std::string>(field);
        std::memcpy(data + offset, str.c_str(), CHAR_SIZE);
        offset += CHAR_SIZE;
        break;
      }
    }
  }
}

db::TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
  // TODO pa2: implement
  std::vector<field_t> fields;
  size_t offset = 0;

  for (const auto &type : types) {
    switch (type) {
      case type_t::INT: {
        int intValue;
        std::memcpy(&intValue, data + offset, INT_SIZE);
        fields.emplace_back(intValue);
        offset += INT_SIZE;
        break;
      }
      case type_t::DOUBLE: {
        double doubleValue;
        std::memcpy(&doubleValue, data + offset, DOUBLE_SIZE);
        fields.emplace_back(doubleValue);
        offset += DOUBLE_SIZE;
        break;
      }
      case type_t::CHAR: {
        char charValue[CHAR_SIZE];
        std::memcpy(charValue, data + offset, CHAR_SIZE);
        fields.emplace_back(std::string(charValue, CHAR_SIZE));
        offset += CHAR_SIZE;
        break;
      }
    }
  }

  return Tuple(fields);
}
