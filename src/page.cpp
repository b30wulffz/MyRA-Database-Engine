#include "global.h"
/**
 * @brief Construct a new Page object. Never used as part of the code
 *
 */
Page::Page()
{
    this->pageName = "";
    this->tableName = "";
    this->pageIndex = -1;
    this->pageRowIndex = -1;
    this->pageColIndex = -1;
    this->rowCount = 0;
    this->columnCount = 0;
    this->rows.clear();
    // todo: maintain a row loaded bool to reduce redundant row loads
}

/**
 * @brief Construct a new Page:: Page object given the table name and page
 * index. When tables are loaded they are broken up into blocks of BLOCK_SIZE
 * and each block is stored in a different file named
 * "<tablename>_Page<pageindex>". For example, If the Page being loaded is of
 * table "R" and the pageIndex is 2 then the file name is "R_Page2". The page
 * loads the rows (or tuples) into a vector of rows (where each row is a vector
 * of integers).
 *
 * @param tableName 
 * @param pageIndex 
 */
Page::Page(string tableName, int pageIndex) // SP: Loading a page stored in temp
{
    logger.log("Page::Page");
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->pageName = "../data/temp/" + this->tableName + "_Page" + to_string(pageIndex);
    Table table = *tableCatalogue.getTable(tableName);
    this->columnCount = table.columnCount;
    uint maxRowCount = table.maxRowsPerBlock;
    vector<int> row(columnCount, 0);
    this->rows.assign(maxRowCount, row);

    ifstream fin(pageName, ios::in); // SP: Loading the stream from stored page
    this->rowCount = table.rowsPerBlockCount[pageIndex];
    int number;
    for (uint rowCounter = 0; rowCounter < this->rowCount; rowCounter++) {
        for (int columnCounter = 0; columnCounter < columnCount; columnCounter++) {
            fin >> number;
            this->rows[rowCounter][columnCounter] = number; // SP: Copying the data in a vector from a page
        }
    }
    fin.close();
}

/**
 * @brief Construct a new Page:: Page object given the matrix name, page row 
 * index and page col index. When tables are loaded they are broken up into 
 * blocks of 44*44 and each block is stored in a different file named
 * "<matrixname>_Page_R<pageRowIndex>_C<pageColIndex>". For example, If the 
 * Page being loaded is of matrix "M", the pageRowIndex is 3 and the pageColIndex 
 * is 2 then the file name is "M_Page_R3_C2". The page loads the rows (or 
 * tuples) into a vector of rows (where each row is a vector of integers).
 *
 * @param tableName 
 * @param pageRowIndex 
 * @param pageColIndex 
 */
Page::Page(string matrixName, int pageRowIndex, int pageColIndex) // SP: Loading a page stored in temp
{
    logger.log("Page::Page::Matrix");
    this->tableName = matrixName;
    this->pageRowIndex = pageRowIndex;
    this->pageColIndex = pageColIndex;
    this->pageName = "../data/temp/" + this->tableName + "_Page_R" + to_string(pageRowIndex) + "_C" + to_string(pageColIndex);
    this->columnCount = 0;
    this->rowCount = 0;
    this->rows.clear();

    // this->rows.assign(maxRowCount, row);

    ifstream fin(this->pageName, ios::in); // SP: Loading the stream from stored page
    string line, value;
    
    bool colCnt = true;
    while(getline(fin, line)){
        stringstream rowStream(line);
        vector<int> row;
        while(getline(rowStream, value, ' ')){
            row.push_back(stoi(value));
            if(colCnt){
                this->columnCount++;
            }
        }
        colCnt = false;
        this->rows.push_back(row);
        this->rowCount++;
    }

    fin.close();
}

/**
 * @brief Get row from page indexed by rowIndex
 * 
 * @param rowIndex 
 * @return vector<int> 
 */
vector<int> Page::getRow(int rowIndex)
{
    logger.log("Page::getRow");
    vector<int> result;
    result.clear();
    
    if (rowIndex >= this->rows.size())
        return result;
    return this->rows[rowIndex];
}

/**
 * @brief Returns all the rows present in page
 * 
 * @return vector<vector<int>> 
 */
vector<vector<int>> Page::getRows()
{
    return this->rows;
}

Page::Page(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount) // SP: generating a page from data
{
    logger.log("Page::Page");
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->rows = rows;
    this->rowCount = rowCount;
    this->columnCount = rows[0].size();
    this->pageName = "../data/temp/" + this->tableName + "_Page" + to_string(pageIndex);
}

Page::Page(string tableName, int pageRowIndex, int pageColIndex, vector<vector<int>> rows, int rowCount) // SP: generating a page from data
{
    logger.log("Page::Page::Matrix");
    this->tableName = tableName;
    this->pageRowIndex = pageRowIndex;
    this->pageColIndex = pageColIndex;
    this->rows = rows;
    this->rowCount = rowCount;
    this->columnCount = rows[0].size();
    this->pageName = "../data/temp/" + this->tableName + "_Page_R" + to_string(pageRowIndex) + "_C" + to_string(pageColIndex);
}

/**
 * @brief writes current page contents to file.
 * 
 */
void Page::writePage()
{
    logger.log("Page::writePage");
    ofstream fout(this->pageName, ios::trunc);
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++) // SP: writing just the data in page
    {
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++) {
            if (columnCounter != 0)
                fout << " ";
            fout << this->rows[rowCounter][columnCounter];
        }
        fout << endl;
    }
    fout.close();
}


/**
 * @brief appends a row to the page.
 * 
 */
void Page::appendToPage(vector<int> row)
{
    logger.log("Page::appendToPage");
    ofstream fout(this->pageName, ios::app);
    if(this->columnCount == 0){
        this->columnCount = row.size();
    }
    if(this->columnCount == row.size()){
        for(int columnCounter=0; columnCounter<row.size(); columnCounter++){
            if(columnCounter != 0)
                fout << " ";
            fout << row[columnCounter];
        }
        if(row.size()>0)
            fout << "\n";
        // set it optional
        this->rows.push_back(row);
        this->rowCount++;
    }
    fout.close();
}

/**
 * @brief clears a page if it exists
 * 
 */
void Page::clearPage()
{
    ifstream file(this->pageName);
    if(file){
        ofstream writeFile(this->pageName);
        writeFile.close();
    }
    file.close();
}