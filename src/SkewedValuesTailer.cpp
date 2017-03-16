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
 * File:   SkewedValuesTailer.cpp
 * Author: Fabio Buso <buso@kth.se>
 *
 */

#include "SkewedValuesTailer.h"

using namespace Utils;
using namespace Utils::NdbC;

const string skv_table = "SKEWED_VALUES";
const int skv_noCols = 3;
const string skv_cols[skv_noCols] =
    {
      "SD_ID_OID",
      "STRING_LIST_ID_EID",
      "INTEGER_IDX",
    };

const int skv_noEvents = 1;
const NdbDictionary::Event::TableEvent skv_events[skv_noEvents] = {NdbDictionary::Event::TE_DELETE};

const WatchTable SkewedValuesTailer::TABLE = {skv_table, skv_cols, skv_noCols, skv_events, skv_noEvents};

SkewedValuesTailer::SkewedValuesTailer(Ndb* ndb, const int poll_maxTimeToWait)
  :SkewedTailer(ndb, TABLE, poll_maxTimeToWait) { }
