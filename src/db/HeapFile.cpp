#include <db/Database.hpp>
#include <db/HeapFile.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapFile::HeapFile(const std::string &name, const TupleDesc &td) : DbFile(name, td) {}

void HeapFile::insertTuple(const Tuple &t) {
  // TODO pa2: implement
  // Get the database buffer pool
  BufferPool &bufferPool = getDatabase().getBufferPool();

  // Try to insert into the last page
  if (numPages > 0) {
    PageId lastPageId = {name, numPages - 1};
    Page &lastPage = bufferPool.getPage(lastPageId);
    HeapPage lastHeapPage(lastPage, td);

    if (lastHeapPage.insertTuple(t)) {
      bufferPool.markDirty(lastPageId);
      return;
    }
  }
  // If last page is full or there are no pages, create a new page
  Page newPage = {};
  HeapPage newHeapPage(newPage, td);
  if (!newHeapPage.insertTuple(t)) {
    throw std::runtime_error("Failed to insert tuple into new page.");
  }

  // Write the new page to the file and update the buffer pool
  writePage(newPage, numPages);
  numPages++;

  // Mark the new page as dirty
  PageId newPageId = {name, numPages - 1};
  bufferPool.markDirty(newPageId);
}



void HeapFile::deleteTuple(const Iterator &it) {
  // TODO pa2: implement
  // Get the database buffer pool
  BufferPool &bufferPool = getDatabase().getBufferPool();

  // Get the page containing the tuple
  PageId pageId = {name, it.page};
  Page &page = bufferPool.getPage(pageId);
  HeapPage heapPage(page, td);

  // Delete the tuple at the given slot
  heapPage.deleteTuple(it.slot);
  bufferPool.markDirty(pageId);
}

Tuple HeapFile::getTuple(const Iterator &it) const {
  // TODO pa2: implement
  // Get the database buffer pool
  BufferPool &bufferPool = getDatabase().getBufferPool();
  // Check if the page ID is within valid range
  if (it.page >= numPages) {
    throw std::out_of_range("Page id " + std::to_string(it.page) + " out of range.");
  }
  
  // Get the page containing the tuple
  PageId pageId = {name, it.page};
  Page &page = bufferPool.getPage(pageId);
  HeapPage heapPage(page, td);

  // Return the tuple at the given slot
  return heapPage.getTuple(it.slot);
}

void HeapFile::next(Iterator &it) const {
  // TODO pa2: implement
  // Get the database buffer pool
  BufferPool &bufferPool = getDatabase().getBufferPool();

  while (it.page < numPages) {
    PageId pageId = {name, it.page};
    Page &page = bufferPool.getPage(pageId);
    HeapPage heapPage(page, td);

    heapPage.next(it.slot);

    if (it.slot < heapPage.end()) {
      return;  // Found the next tuple
    }

    // Move to the next page
    it.page++;
    it.slot = heapPage.begin();
  }

  // If no more tuples, set iterator to end position
  it.page = numPages;
  it.slot = 0;
}

Iterator HeapFile::begin() const {
  // TODO pa2: implement
  size_t pageId = 0;

  // Get the database buffer pool
  BufferPool &bufferPool = getDatabase().getBufferPool();

  // Iterate over pages to find the first non-empty page
  while (pageId < numPages) {
    PageId pid = {name, pageId};
    Page &page = bufferPool.getPage(pid);
    HeapPage heapPage(page, td);

    size_t firstSlot = heapPage.begin();
    if (firstSlot != heapPage.end()) {
      return Iterator(*this, pageId, firstSlot);
    }

    pageId++;
  }

  // If no tuples, return an iterator to the end
  return end();
}

Iterator HeapFile::end() const {
  // TODO pa2: implement
  return Iterator(*this, numPages, 0);

}