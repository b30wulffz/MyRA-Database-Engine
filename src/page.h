#include "logger.h"
/**
 * @brief The Page object is the main memory representation of a physical page
 * (equivalent to a block). The page class and the page.h header file are at the
 * bottom of the dependency tree when compiling files. 
 *<p>
 * Do NOT modify the Page class. If you find that modifications
 * are necessary, you may do so by posting the change you want to make on Moodle
 * or Teams with justification and gaining approval from the TAs. 
 *</p>
 */

enum FileType {
    TABLE,
    MATRIX,
    SPARSE_MATRIX,
    OTHER
};

class Page {

    string tableName;
    int pageIndex;
    bool isLoaded = false;
    vector<vector<int>> rows;

public:
    FileType type = OTHER;
    string pageName = "";
    int rowCount;
    int pageRowIndex;
    int pageColIndex;
    int columnCount;
    Page();
    Page(string tableName, int pageIndex);
    Page(string matrixName, int pageIndex, bool isSparse);
    Page(string matrixName, int pageRowIndex, int pageColIndex, bool isMatrix);
    Page(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount);
    Page(string matrixName, int pageIndex, vector<vector<int>> rows, int rowCount, bool isSparse);
    Page(string matrixName, int pageRowIndex, int pageColIndex, vector<vector<int>> rows, int rowCount);
    Page(string matrixName, int pageIndex, vector<vector<int>> rows);
    vector<int> getRow(int rowIndex);
    void writePage();
    vector<vector<int>> &getRows();
    void appendToPage(vector<int> row);
    void clearPage();
    void loadRows();
};