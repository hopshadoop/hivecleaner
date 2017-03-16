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
 * File:   main.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */
#include "Notifier.h"
#include <boost/program_options.hpp>
#include "Version.h"

namespace po = boost::program_options;

int main(int argc, char** argv) {

    string connection_string;
    string database_name = "metastore";
    int poll_maxTimeToWait = 2000;
    int log_level = 2;

    bool recovery = false;
    bool stats = false;

    po::options_description desc("Allowed options");
    desc.add_options()
            ("help", "produce help message")
            ("connection", po::value<string>(&connection_string), "connection string/ ndb host")
            ("database", po::value<string>(&database_name)->default_value(database_name), "database name.")
            ("poll_maxTimeToWait", po::value<int>(&poll_maxTimeToWait)->default_value(poll_maxTimeToWait), "max time to wait in miliseconds while waiting for events in pollEvents")
            ("log_level", po::value<int>(&log_level)->default_value(log_level), "log level trace=0, debug=1, info=2, warn=3, error=4, fatal=5")
            ("recovery", po::value<bool>(&recovery)->default_value(recovery), "enable or disable startup recovery")
            ("stats", po::value<bool>(&stats)->default_value(stats), "enable or disable print of accumulators stats")
            ("version", "ePipe version")
            ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        cout << desc << endl;
        return EXIT_SUCCESS;
    }

    if (vm.count("version")) {
        cout << "ePipe " << HIVECLEANER_VERSION_MAJOR << "."  << HIVECLEANER_VERSION_MINOR
                << "." << HIVECLEANER_VERSION_BUILD << endl;
        return EXIT_SUCCESS;
    }

    Logger::setLoggerLevel(log_level);

    if(connection_string.empty() || database_name.empty()){
        LOG_ERROR("you should provide at least connection and database");
        return EXIT_FAILURE;
    }

    Notifier *notifer = new Notifier(connection_string.c_str(), database_name.c_str(), poll_maxTimeToWait, recovery);
    notifer->start();

    return EXIT_SUCCESS;
}
