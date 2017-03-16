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
 * File:   SkewedLocTailer.cpp
 * Author: Fabio Buso <buso@kth.se>
 *
 */

#include "SkewedLocTailer.h"

using namespace Utils;
using namespace Utils::NdbC;

const string skl_table = "SKEWED_COL_VALUE_LOC_MAP";
const int skl_noCols = 3;
const string skl_cols[skl_noCols] =
    {
      "SD_ID",
      "STRING_LIST_ID_KID",
      "LOCATION",
    };

const int skl_noEvents = 1;
const NdbDictionary::Event::TableEvent skl_events[skl_noEvents] = {NdbDictionary::Event::TE_DELETE};

const WatchTable SkewedLocTailer::TABLE = {skl_table, skl_cols, skl_noCols, skl_events, skl_noEvents};

SkewedLocTailer::SkewedLocTailer(Ndb* ndb, const int poll_maxTimeToWait)
  :SkewedTailer(ndb, TABLE, poll_maxTimeToWait) { }
