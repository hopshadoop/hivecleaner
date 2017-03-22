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

#include "array_adapter.hpp"
#include "common.h"

char* ReadWriteArrayAdapter::make_aRef(const NdbDictionary::Column* column,
          std::string input,
          ErrorType& error)
{
  char* new_ref;
  char* data_start;

  /*
   Allocate bytes and push them into the aRef_created vector.
   After this operation, new_ref has a complete aRef to use in insertion
   and data_start has ptr from which data is to be written.
   The new_aref returned is padded completely with blank spaces.
   */
  allocate_in_bytes(column, new_ref, data_start, input.length(), error);

  if(error != Success)
  {
    return NULL;
  }

  /*
   Copy the input string into aRef's data pointer
   without affecting remaining blank spaces at end.
   */
  strncpy(data_start, input.c_str(), input.length());

  return new_ref;
}

void ReadWriteArrayAdapter::allocate_in_bytes(const NdbDictionary::Column* column,
                  char*& aRef,
                  char*& first_byte,
                  size_t bytes,
                  ErrorType& error)
{
  bool is_binary;
  char zero_char;
  NdbDictionary::Column::ArrayType array_type;
  size_t max_length;

  /* unless there is going to be any problem */
  error = Success;

  if (column == NULL)
  {
    error = InvalidNullColumn;
    aRef = NULL;
    first_byte = NULL;
    return;
  }

  if (!is_array_type(column->getType()))
  {
    error = InvalidColumnType;
    aRef = NULL;
    first_byte = NULL;
    return;
  }

  is_binary = is_binary_array_type(column->getType());
  zero_char = (is_binary ? 0 : ' ');
  array_type = column->getArrayType();
  max_length = column->getLength();

  if (bytes > max_length)
  {
    error = BytesOutOfRange;
    aRef = NULL;
    first_byte = NULL;
    return;
  }

  switch (array_type) {
  case NdbDictionary::Column::ArrayTypeFixed:
    /* no need to store length bytes */
    aRef = new char[max_length];
    first_byte = aRef;
    /* pad the complete string with blank space (or) null bytes */
    for (size_t i=0; i < max_length; i++) {
      aRef[i] = zero_char;
    }
    break;
  case NdbDictionary::Column::ArrayTypeShortVar:
    /* byte length stored over first byte. no padding required */
    aRef = new char[1 + bytes];
    first_byte = aRef + 1;
    aRef[0] = (char)bytes;
    break;
  case NdbDictionary::Column::ArrayTypeMediumVar:
    /* byte length stored over first two bytes. no padding required */
    aRef = new char[2 + bytes];
    first_byte = aRef + 2;
    aRef[0] = (char)(bytes % 256);
    aRef[1] = (char)(bytes / 256);
    break;
  }
  aRef_created.push_back(aRef);
}


string ReadOnlyArrayAdapter::get_string(const NdbRecAttr* attr,
                                             ErrorType& error) const
{
  size_t attr_bytes= 0;
  const char* data_ptr= NULL;
  std::string result= "";

  /* get the beginning of data and its size.. */
  get_byte_array(attr, data_ptr, attr_bytes, error);

  if(error != Success)
  {
    return result;
  }

  /* ..and copy the  value into result */
  result = string(data_ptr, attr_bytes);

  /* special treatment for FixedArrayType to eliminate padding characters */
  if(attr->getColumn()->getArrayType() == NdbDictionary::Column::ArrayTypeFixed)
  {
    char padding_char = ' ';
    std::size_t last = result.find_last_not_of(padding_char);
    result = result.substr(0, last+1);
  }

  return result;
}


void ReadOnlyArrayAdapter::get_byte_array(const NdbRecAttr* attr,
               const char*& data_ptr,
               size_t& bytes,
               ErrorType& error) const
{
  /* unless there is a problem */
  error= Success;

  if (attr == NULL)
  {
    error = InvalidNullAttribute;
    return;
  }

  if (!is_array_type(attr->getType()))
  {
    error = InvalidColumnType;
    return;
  }

  const NdbDictionary::Column::ArrayType array_type =
      attr->getColumn()->getArrayType();
  const size_t attr_bytes = attr->get_size_in_bytes();
  const char* aRef = attr->aRef();

  if(aRef == NULL)
  {
    error= InvalidNullaRef;
    return;
  }

  switch (array_type) {
  case NdbDictionary::Column::ArrayTypeFixed:
    /* no length bytes stored with aRef */
    data_ptr = aRef;
    bytes = attr_bytes;
    break;
  case NdbDictionary::Column::ArrayTypeShortVar:
    /* first byte of aRef has length of the data */
    data_ptr = aRef + 1;
    bytes = (size_t)(aRef[0]);
    break;
  case NdbDictionary::Column::ArrayTypeMediumVar:
    /* first two bytes of aRef has length of the data */
    data_ptr = aRef + 2;
    bytes = (size_t)(aRef[1]) * 256 + (size_t)(aRef[0]);
    break;
  default:
    /* should never reach here */
    data_ptr = NULL;
    bytes = 0;
    error = InvalidArrayType;
    break;
  }
}


bool ReadOnlyArrayAdapter::is_binary_array_type(const NdbDictionary::Column::Type t) const
{
  bool is_binary;

  switch (t)
  {
  case NdbDictionary::Column::Binary:
  case NdbDictionary::Column::Varbinary:
  case NdbDictionary::Column::Longvarbinary:
    is_binary = true;
    break;
  default:
    is_binary = false;
  }
  return is_binary;
}


bool ReadOnlyArrayAdapter::is_array_type(const NdbDictionary::Column::Type t) const
{
  bool is_array;

  switch (t)
  {
  case NdbDictionary::Column::Binary:
  case NdbDictionary::Column::Varbinary:
  case NdbDictionary::Column::Longvarbinary:
  case NdbDictionary::Column::Char:
  case NdbDictionary::Column::Varchar:
  case NdbDictionary::Column::Longvarchar:
    is_array = true;
    break;
  default:
    is_array = false;
  }
  return is_array;
}
