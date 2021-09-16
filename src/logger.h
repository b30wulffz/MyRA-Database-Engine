#include <bits/stdc++.h>
#include <fstream>
#include <iostream>
#include <sys/stat.h>

using namespace std;

class Logger {

    string logFile = "log";
    ofstream fout;

public:
    Logger();
    void log(string logString);
};

extern Logger logger;