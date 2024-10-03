#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
  // TODO pa2: initialize private members
  // NOTE: header and data should point to locations inside the page buffer. Do not allocate extra memory.
  // Initialize header and data pointers
  capacity = (DEFAULT_PAGE_SIZE * 8) / (td.length() * 8 + 1);  // Calculate number of slots
  header = page.data();  // Header is at the beginning of the page
  data = page.data() + (capacity + 7) / 8;  // Header is stored in bits, so divide by 8 to get bytes

  // Zero out the header initially
  count = 0;
  for(size_t i = 0; i < capacity; i++){
    if(!empty(i)){
      count++;
    }
  }
  //count++;
}

size_t HeapPage::begin() const {
  // TODO pa2: implement
  for (size_t i = 0; i < capacity; ++i) {
    if (!empty(i)) {
      return i;
    }
  }
  return capacity;
}

size_t HeapPage::end() const {
  // TODO pa2: implement
  return capacity;
}

bool HeapPage::insertTuple(const Tuple &t) {
  // TODO pa2: implement
  for (size_t i = 0; i < capacity; ++i) {
    if (empty(i)) {
      // Mark slot as used in the header
      int bitIndex = 7 - i % 8;
      header[i / 8] |= (1 << bitIndex);
      count++;
      // Serialize the tuple into the data section
      td.serialize(data + i * td.length(), t);
      return true;
    }
  }
  return false;  // No empty slots available
}

void HeapPage::deleteTuple(size_t slot) {
  // TODO pa2: implement
  if (slot >= capacity) {
    throw std::out_of_range("Slot out of range.");
  }
  if (empty(slot)) {
    throw std::logic_error("Slot is already empty.");
  }

  // Mark the slot as empty in the header
  int bitIndex = 7 - slot % 8;
  header[slot / 8] &= ~(1 << bitIndex);
  count --;
}

Tuple HeapPage::getTuple(size_t slot) const {
  // TODO pa2: implement
  if (slot >= capacity || empty(slot)) {
    throw std::runtime_error("Slot is empty or out of range.");
  }

  // Deserialize the tuple from the data section
  return td.deserialize(data + slot * td.length());
}

void HeapPage::next(size_t &slot) const {
  // TODO pa2: implement
  for (++slot; slot < capacity; ++slot) {
    if (!empty(slot)) {
      return;
    }
  }
  slot = capacity;  // Set to end if no more populated slots are found
}

bool HeapPage::empty(size_t slot) const {
  // TODO pa2: implement
  if (slot >= capacity) {
    return true;
  }

  // Check the corresponding bit in the header
  int bitIndex = 7 - slot % 8;
  return (header[slot / 8] & (1 << bitIndex)) == 0;
}
