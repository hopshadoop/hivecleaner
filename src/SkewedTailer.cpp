/*
 * Copyright (C) 2017 Hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

/*
 * File:   SkewedTailer.cpp
 * Author: Fabio Buso <buso@kth.se>
 *
 */

#include "SkewedTailer.h"

using namespace Utils;
using namespace Utils::NdbC;

const string skewed_values = "SKEWED_VALUES";
const string loc_map = "SKEWED_COL_VALUE_LOC_MAP";
const string skewed_string = "SKEWED_STRING_LIST";

const int fk = 1; //In both table sthe fk to SKEWED_STRING_LIST is the 1st column

void SkewedTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]){
  if (check(preValue, fk, skewed_values.c_str()) && check(preValue, fk, loc_map.c_str())) {
    delEntries(preValue[fk], skewed_string.c_str());
  }
}
