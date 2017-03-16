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
 * File:   Cleaner.h
 * Author: Fabio Buso <buso@kth.se>
 *
 */

#ifndef CLEANER_H
#define CLEANER_H

#include "TableTailer.h"

class Cleaner: public TableTailer{
public:
    Cleaner(Ndb* ndb, const WatchTable table, const int poll_maxTimeToWait)
      :TableTailer(ndb, table, poll_maxTimeToWait) { };
    virtual ~Cleaner() { };
protected:
    bool check(NdbRecAttr**, int, const char*);
    void delEntries(NdbRecAttr*, const char*);
};

#endif /* CLEANER_H */
