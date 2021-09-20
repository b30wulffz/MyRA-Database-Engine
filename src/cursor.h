#include "bufferManager.h"
/**
 * @brief The cursor is an important component of the system. To read from a
 * table, you need to initialize a cursor. The cursor reads rows from a page one
 * at a time.
 *
 */
class Cursor {
public:
    Page* page; // ff: use pointer
    int pageIndex = -1;
    int pageRowIndex = -1;
    int pageColIndex = -1;
    string tableName;
    int pagePointer;
    string currentStruct; // table, matrix

public:
    Cursor(string tableName, int pageIndex);
    Cursor(string matrixName, int pageRowIndex, int pageColIndex);
    vector<int> getNext();
    void nextPage(int pageIndex);
    void nextPage(int pageRowIndex, int pageColIndex);
};