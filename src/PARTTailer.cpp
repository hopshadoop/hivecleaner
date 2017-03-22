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
 * File:   PARTTailer.cpp
 * Author: Fabio Buso <buso@kth.se>
 *
 */

#include "PARTTailer.h"

using namespace Utils;
using namespace Utils::NdbC;

const string part_table = "PARTITIONS";
const int part_noCols = 6;
const string part_cols[part_noCols] =
    {
      "PART_ID",
      "CREATE_TIME",
      "LAST_ACCESS_TIME",
      "PART_NAME",
      "SD_ID",
      "TBL_ID"
    };

enum column_idx{
  PART_ID,
  SD_ID=4,
};

const int part_noEvents = 1;
const NdbDictionary::Event::TableEvent part_events[part_noEvents] = {NdbDictionary::Event::TE_DELETE};

const WatchTable PARTTailer::TABLE = {part_table, part_cols, part_noCols, part_events, part_noEvents};

const string sds_table = "SDS";

PARTTailer::PARTTailer(Ndb* ndb, const int poll_maxTimeToWait)
  :Cleaner(ndb, TABLE, poll_maxTimeToWait) { }

void PARTTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]){
  LOG_INFO("Delete PART event received. Primary Key value: " << preValue[PART_ID]->u_64_value());

  delEntries(preValue[SD_ID], sds_table.c_str());
}

PARTTailer::~PARTTailer() { }
