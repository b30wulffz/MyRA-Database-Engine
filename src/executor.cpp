#include "global.h"

void executeCommand()
{

    switch (parsedQuery.queryType) {
    case CLEAR:
        executeCLEAR();
        break;
    case CROSS:
        executeCROSS();
        break;
    case DISTINCT:
        executeDISTINCT();
        break;
    case EXPORT:
        executeEXPORT();
        break;
    case INDEX:
        executeINDEX();
        break;
    case JOIN:
        executeJOIN();
        break;
    case LIST:
        executeLIST();
        break;
    case LOAD:
        executeLOAD();
        break;
    case PRINT:
        executePRINT();
        break;
    case PROJECTION:
        executePROJECTION();
        break;
    case RENAME:
        executeRENAME();
        break;
    case SELECTION:
        executeSELECTION();
        break;
    case SORT:
        executeSORT();
        break;
    case SOURCE:
        executeSOURCE();
        break;
    case LOADMAT:
        executeLOADMAT();
        break;
    case PRINTMAT:
        executePRINTMAT();
        break;
    case EXPORTMAT:
        executeEXPORTMAT();
        break;
    case TRANSPOSEMAT:
        executeTRANSPOSEMAT();
        break;
    case GROUPBY:
        executeGROUPBY();
        break;
    default:
        cout << "PARSING ERROR" << endl;
    }

    return;
}

void printRowCount(int rowCount)
{
    cout << "\n\nRow Count: " << rowCount << endl;
    return;
}