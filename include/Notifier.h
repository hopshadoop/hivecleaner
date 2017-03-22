/*
 * Copyright (C) 2016 Hops.io
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
 * File:   Notifier.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef NOTIFIER_H
#define NOTIFIER_H

#include "SDSTailer.h"
#include "SkewedLocTailer.h"
#include "SkewedValuesTailer.h"
#include "IDXSTailer.h"
#include "TBLSTailer.h"
#include "PARTTailer.h"

class Notifier {
public:
    Notifier(const char* connection_string, const char* database_name, const int poll_maxTimeToWait, const bool recovery);
    void start();
    virtual ~Notifier();

private:
    const char* mDatabaseName;

    Ndb_cluster_connection *mClusterConnection;

    const int mPollMaxTimeToWait;
    const bool mRecovery;

    SDSTailer* mSDSTailer;
    SkewedLocTailer* mSklTailer;
    SkewedValuesTailer* mSkvTailer;
    IDXSTailer* mIdxsTailer;
    TBLSTailer* mTblsTailer;
    PARTTailer* mPartTailer;

    Ndb* create_ndb_connection(const char* database);
    Ndb_cluster_connection* connect_to_cluster(const char *connection_string);

    void setup();
};

#endif /* NOTIFIER_H */
