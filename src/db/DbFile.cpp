#include <db/DbFile.hpp>
#include <stdexcept>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include<cstring>

using namespace db;

const TupleDesc &DbFile::getTupleDesc() const { return td; }

DbFile::DbFile(const std::string &name, const TupleDesc &td) : name(name), td(td) {
  // TODO pa2: open file and initialize numPages
  // Hint: use open, fstat
  fileDescriptor = open(name.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (fileDescriptor < 0) {
    throw std::runtime_error("Failed to open file: " + name);
  }

  // Get the file size using fstat
  struct stat fileStats;
  if (fstat(fileDescriptor, &fileStats) < 0) {
    throw std::runtime_error("Failed to get file stats for: " + name);
  }

  // Calculate number of pages in the file based on size
  numPages = fileStats.st_size / DEFAULT_PAGE_SIZE;

  // If the file is empty (size is zero), create an initial page
  if (numPages == 0) {
    Page emptyPage = {};
    memset(&emptyPage, 0, sizeof(Page));  // Zero out the page data
    writePage(emptyPage, 0);
    numPages = 1;
  }
}

DbFile::~DbFile() {
  // TODO pa2: close file
  // Hind: use close
  if (fileDescriptor >= 0) {
    close(fileDescriptor);
  }
}

const std::string &DbFile::getName() const { return name; }

void DbFile::readPage(Page &page, const size_t id) const {
  reads.push_back(id);
  // TODO pa2: read page
  // Hint: use pread
    // Check if the page ID is within the valid range
  if (id >= numPages) {
    throw std::out_of_range("Page id " + std::to_string(id) + " out of range.");
  }

  // Calculate the offset in bytes for the given page ID
  size_t offset = id * DEFAULT_PAGE_SIZE;

  // Read the page using pread
  ssize_t bytesRead = pread(fileDescriptor, page.data(), DEFAULT_PAGE_SIZE, offset);
  if (bytesRead != DEFAULT_PAGE_SIZE) {
    throw std::runtime_error("Failed to read page " + std::to_string(id) + " from file: " + name);
  }
}

void DbFile::writePage(const Page &page, const size_t id) const {
  writes.push_back(id);
  // TODO pa2: write page
  // Hint: use pwrite
    // Check if the page ID is within the valid range
  if (id >= numPages) {
    throw std::out_of_range("Page id " + std::to_string(id) + " out of range.");
  }

  // Calculate the offset in bytes for the given page ID
  size_t offset = id * DEFAULT_PAGE_SIZE;

  // Write the page using pwrite
  ssize_t bytesWritten = pwrite(fileDescriptor, page.data(), DEFAULT_PAGE_SIZE, offset);
  if (bytesWritten != DEFAULT_PAGE_SIZE) {
    throw std::runtime_error("Failed to write page " + std::to_string(id) + " to file: " + name);
  }
}

const std::vector<size_t> &DbFile::getReads() const { return reads; }

const std::vector<size_t> &DbFile::getWrites() const { return writes; }

void DbFile::insertTuple(const Tuple &t) { throw std::runtime_error("Not implemented"); }

void DbFile::deleteTuple(const Iterator &it) { throw std::runtime_error("Not implemented"); }

Tuple DbFile::getTuple(const Iterator &it) const { throw std::runtime_error("Not implemented"); }

void DbFile::next(Iterator &it) const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::begin() const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::end() const { throw std::runtime_error("Not implemented"); }

size_t DbFile::getNumPages() const { return numPages; }
