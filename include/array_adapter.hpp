/*
Copyright (c) 2014 Oracle and/or its affiliates. All rights reserved.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef ARRAY_ADAPTER_HPP
#define ARRAY_ADAPTER_HPP

#include "common.h"
#include <algorithm>
#include <assert.h>

/*
 Utility classes to convert between C++ strings/byte arrays and the
 internal format used for [VAR]CHAR/BINARY types.

 Base class that can be used for read operations. The column type is
 taken from the NdbRecAttr object, so only one object is needed to
 convert from different [VAR]CHAR/BINARY types. No additional memory
 is allocated.
 */
class ReadOnlyArrayAdapter {
public:
  ReadOnlyArrayAdapter() {}

  enum ErrorType {Success,
                  InvalidColumnType,
                  InvalidArrayType,
                  InvalidNullColumn,
                  InvalidNullAttribute,
                  InvalidNullaRef,
                  BytesOutOfRange,
                  UnknownError};

  /*
    Return a C++ string from the aRef() value of attr. This value
    will use the column and column type from attr. The advantage is
    for reading; the same ArrayAdapter can be used for multiple
    columns. The disadvantage is; passing an attribute not of
    [VAR]CHAR/BINARY type will result in a traditional exit(-1)
    */
  std::string get_string(const NdbRecAttr* attr,
                         ErrorType& error) const;

  /* Calculate the first_byte and number of bytes in aRef for attr */
  void get_byte_array(const NdbRecAttr* attr,
                      const char*& first_byte,
                      size_t& bytes,
                      ErrorType& error) const;

  /* Check if a column is of type [VAR]BINARY */
  bool is_binary_array_type(const NdbDictionary::Column::Type t) const;

  /* Check if a column is of type [VAR]BINARY or [VAR]CHAR */
  bool is_array_type(const NdbDictionary::Column::Type t) const;
private:
  /* Disable copy constructor */
  ReadOnlyArrayAdapter(const ReadOnlyArrayAdapter& a) {}
};


 /*
  Extension to ReadOnlyArrayAdapter to be used together with
  insert/write/update operations. Memory is allocated for each
  call to make_aRef or allocate_in_bytes. The memory allocated will
  be deallocated by the destructor. To save memory, the scope of an
  instance of this class should not be longer than the life time of
  the transaction. On the other hand, it must be long enough for the
  usage of all references created
  */
class ReadWriteArrayAdapter : public ReadOnlyArrayAdapter {
public:
  ReadWriteArrayAdapter() {}

  /* Destructor, the only place where memory is deallocated */
  ~ReadWriteArrayAdapter();

  /*
   Create a binary representation of the string 's' and return a
   pointer to it. This pointer can later be used as argument to for
   example setValue
   */
  char* make_aRef(const NdbDictionary::Column* column,
                  std::string s,
                  ErrorType& error);

  /*
   Allocate a number of bytes suitable for this column type. aRef
   can later be used as argument to for example setValue. first_byte
   is the first byte to store data to. bytes is the number of bytes
   to allocate
   */
  void allocate_in_bytes(const NdbDictionary::Column* column,
                         char*& aRef,
                         char*& first_byte,
                         size_t bytes,
                         ErrorType& error);

private:
  /* Disable copy constructor */
  ReadWriteArrayAdapter(const ReadWriteArrayAdapter& a)
    :ReadOnlyArrayAdapter() {}

  /* Record of allocated char arrays to delete by the destructor */
  std::vector<char*> aRef_created;
};


inline ReadWriteArrayAdapter::~ReadWriteArrayAdapter()
{
  for (std::vector<char*>::iterator i = aRef_created.begin();
       i != aRef_created.end();
       ++i) {
    delete [] *i;
  }
}


#endif // #ifndef ARRAY_ADAPTER_HPP
