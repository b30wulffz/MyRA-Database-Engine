#include "global.h"

/**
 * @brief Construct a new Matrix:: Matrix object
 *
 */
Matrix::Matrix()
{
    logger.log("Matrix::Matrix");
}

/**
 * @brief Construct a new Matrix:: Matrix object used in the case where the data
 * file is available and LOAD command has been called. This command should be
 * followed by calling the load function;
 *
 * @param matrixName 
 */
Matrix::Matrix(string matrixName)
{
    logger.log("Matrix::Matrix");
    this->sourceFileName = "../data/" + matrixName + ".csv";
    this->matrixName = matrixName;
}

/**
 * @brief Construct a new Matrix:: Matrix object used when an assignment command
 * is encountered. To create the matrix object both the matrix name and the
 * columns the matrix holds should be specified.
 *
 * @param matrixName 
 * @param columns 
 */
// Matrix::Matrix(string matrixName, vector<string> columns)
// {
//     logger.log("Matrix::Matrix");
//     this->sourceFileName = "../data/temp/" + matrixName + ".csv";
//     this->matrixName = matrixName;
//     this->columns = columns;
//     this->columnCount = columns.size();
//     this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * columnCount)); // SP: fitting a row inside a block // fix this in matrix
//     this->writeRow<string>(columns);
// }

/**
 * @brief The load function is used when the LOAD command is encountered. It
 * reads data from the source file, splits it into blocks and updates matrix
 * statistics.
 *
 * @return true if the matrix has been successfully loaded 
 * @return false if an error occurred 
 */
bool Matrix::load()
{
    logger.log("Matrix::load");
    this->calculateStats();
    return this->blockify();
}

/**
 * @brief The calculateStats function is used to count number of row, columns, 
 * and total number of non zero cells.
 * 
 */
// void Matrix::calculateStats()
// {
//     logger.log("Matrix::calculateStats");
//     ifstream fin(this->sourceFileName, ios::in);
//     string line, value;
//     long long nonZeroCount = 0;
//     bool columnCnt = true;
//     while (getline(fin, line)) {
//         stringstream rowStream(line);
//         while (getline(rowStream, value, ',')) {
//             if (stoi(value) != 0)
//                 nonZeroCount++;
//             if (columnCnt)
//                 this->columnCount++;
//         }
//         columnCnt = false;
//         this->rowCount++;
//     }
//     fin.close();
//     long long cellsCount = this->columnCount * this->rowCount;
//     if (nonZeroCount <= (int)(0.4 * cellsCount))
//         this->isSparse = true;
// }

void Matrix::calculateStats()
{
    logger.log("Matrix::calculateStats");
    ifstream fin(this->sourceFileName, ios::in);
    string line, value;
    long long nonZeroCount = 0;

    // count columns
    while (getline(fin, value, ',')) {
        stringstream s(value);
        string filteredValue;
        getline(s, filteredValue, '\n');
        this->columnCount++;
        if(filteredValue != value) break;
    }    

    // seek to start
    fin.clear();
    fin.seekg(0, ios::beg);

    unsigned long long int seekLength = 0;
    uint rowCount = 0;
    uint colCount = 0;

    while (getline(fin, value, ',')) {
        stringstream s(value);
        string filteredValue;
        getline(s, filteredValue, '\n');
        seekLength += filteredValue.size() + 1;
        int num = stoi(filteredValue);
        if(num != 0) nonZeroCount++;
        colCount++;
        if(colCount == this->columnCount){
            colCount = 0;
            // move to next row
            this->rowCount++;
            // tweak for handling \n
            fin.clear();
            fin.seekg(seekLength, ios::beg);
        }
    }
    fin.close();
    
    long long cellsCount = this->columnCount * this->rowCount;
    if (nonZeroCount <= (int)(0.4 * cellsCount))
        this->isSparse = true;
}

/**
 * @brief This function splits all the rows and stores them in multiple files of
 * one block size. 
 *
 * @return true if successfully blockified
 * @return false otherwise
 */
// bool Matrix::blockify_old() // SP: Load matrix in chunk 44*44 : Implement
// {
//     uint chunkCols = 2;
//     uint chunkRows = 2;
//     logger.log("Matrix::blockify");

//     this->colBlocks = (uint)ceil((1.0 * this->columnCount) / chunkCols);
//     this->rowBlocks = (uint)ceil((1.0 * this->rowCount) / chunkRows);

//     long long seekLength = 0;

//     ifstream fin(this->sourceFileName, ios::in);
//     for (uint colBlockIndex = 0; colBlockIndex < this->colBlocks; colBlockIndex++) {
//         string line, value;
//         bool seekLengthFlag = true;
//         long long currentSeek = seekLength;
//         uint rowBlockIndex = 0;
//         uint rowInBlock = 0;
//         vector<vector<int>> block;
//         while (getline(fin, line)) {
//             vector<int> row;
//             stringstream rowStream(line);
//             rowStream.seekg(currentSeek);
//             while (getline(rowStream, value, ',')) {
//                 if (seekLengthFlag)
//                     seekLength += (value.size() + 1LL);
//                 cout << value << " ";
//                 row.push_back(stoi(value));
//                 if (row.size() >= chunkCols)
//                     break;
//             }
//             block.push_back(row);
//             seekLengthFlag = false;
//             rowInBlock++;
//             if (rowInBlock == chunkRows) {
//                 bufferManager.writePage(this->matrixName, rowBlockIndex, colBlockIndex, block, rowInBlock);
//                 rowInBlock = 0;
//                 block.clear();
//                 rowBlockIndex++;
//             }
//         }
//         if (rowInBlock) {
//             bufferManager.writePage(this->matrixName, rowBlockIndex, colBlockIndex, block, rowInBlock);
//             block.clear();
//         }
//         fin.clear();
//         fin.seekg(0, ios::beg);
//     }
//     fin.close();
//     if (this->rowCount == 0)
//         return false;
//     return true;
// }

bool Matrix::blockify() // SP: Load matrix in chunk 44*44 : Implement
{
    logger.log("Matrix::blockify");
    if(this->isSparse){
        // 1 block can hold 1000 rows, each with 2 values
        // this->blockCount = (uint)ceil((this->rowCount*this->columnCount) / 2000.0); 
        this->maxRowsPerBlock = (uint)floor(SPARSE_BLOCK_SIZE / 2.0); // two elements per row
        uint rowCount = 0;
        uint colCount = 0;

        ifstream fin(this->sourceFileName, ios::in);
        string value;

        unsigned long long int seekLength = 0;
        vector<vector<int>> rows;
        this->blockCount = 0;

        while (getline(fin, value, ',')) {
            stringstream s(value);
            getline(s, value, '\n');
            seekLength += value.size() + 1;
            int num = stoi(value);
            if(num != 0){
                int rowcol = (rowCount * this->columnCount) + colCount;
                rows.push_back({rowcol, num});
            }

            colCount++;
            if(colCount == this->columnCount){
                colCount = 0;
                // move to next row
                rowCount++;
                fin.clear();
                fin.seekg(seekLength, ios::beg);
            }
            if(rows.size() == this->maxRowsPerBlock){
                // write page
                bufferManager.writePage(this->matrixName, this->blockCount, rows, rows.size(), true);
                this->blockCount++;
                rows.clear();
            }
        }
        fin.close();
        if(rows.size() > 0){
            // write page
            bufferManager.writePage(this->matrixName, this->blockCount, rows, rows.size(), true);
            this->blockCount++;
            rows.clear();
        }
    }
    else{
        uint chunkCols = BLOCK_COL_COUNT;
        uint chunkRows = BLOCK_ROW_COUNT;

        this->colBlocks = (uint)ceil((1.0 * this->columnCount) / chunkCols);
        this->rowBlocks = (uint)ceil((1.0 * this->rowCount) / chunkRows);

        uint rowBlockIndex = 0;
        uint colBlockIndex = 0;
        uint rowCount = 0;
        uint colCount = 0;

        ifstream fin(this->sourceFileName, ios::in);
        string value;

        unsigned long long int seekLength = 0;
        vector<int> row;

        while (getline(fin, value, ',')) {
            stringstream s(value);
            getline(s, value, '\n');
            seekLength += value.size() + 1;
            int num = stoi(value);
            row.push_back(num);

            colCount++;
            if((colCount%chunkCols) == 0){
                // insert row in rowBlockIndex, colBlockIndex
                bufferManager.appendToPage(this->matrixName, rowBlockIndex, colBlockIndex, row);
                row.clear();
                colBlockIndex++;
            }
            if(colCount == this->columnCount){
                if((colCount%chunkCols) != 0){
                    // insert row in rowBlockIndex, colBlockIndex
                    bufferManager.appendToPage(this->matrixName, rowBlockIndex, colBlockIndex, row);
                    row.clear();
                }
                colCount = 0;
                colBlockIndex = 0;
                // move to next row
                rowCount++;
                if((rowCount%chunkRows) == 0){
                    rowBlockIndex++;
                }
                // tweak for handling \n
                fin.clear();
                fin.seekg(seekLength, ios::beg);
            }
        }
        fin.close();
        
    }
    if (this->rowCount == 0)
        return false;
    return true;
}

/**
 * @brief Function finds if an i,j value exists in any page of the sparse matrix
 * 
 */
int Matrix::findValueAtIndex(int rowInd, int colInd){
    logger.log("Matrix::findValueAtIndex");
    int rowCol = (rowInd * this->columnCount) + colInd;
    Cursor cursor(this->matrixName, 0, true);
    vector<int> row;
    
    for(int pageIndex = 0; pageIndex < this->blockCount; pageIndex++){
        vector<vector<int>> &rows = bufferManager.getPage(this->matrixName, pageIndex, true)->getRows();
        int start = 0, end = rows.size()-1;
        while(start <= end){
            int mid = (start + end) / 2;
            if(rows[mid][0] > rowCol)
                end = mid-1;
            else if(rows[mid][0] < rowCol)
                start = mid+1;
            else
                return rows[mid][1];
        }
    }

    return 0;
}

/**
 * @brief a generic function to be used for both print to cout and export
 * to reduce code duplication
 * 
 */
void Matrix::printToSource(ostream& fout, uint rowCount)
{
    logger.log("Matrix::printToSource");

    if(this->isSparse){
        if(this->isTransposed){ 
            vector<int> row;
            for(int rowIndex = 0; rowIndex < rowCount; rowIndex++){
                for(int colIndex = 0; colIndex < this->columnCount; colIndex++){
                    row.push_back(this->findValueAtIndex(rowIndex, colIndex));
                    if(colIndex == this->columnCount-1 || row.size() == SPARSE_BLOCK_SIZE) // 1000 integers can be stored in 8kB
                    {
                        this->writeRow(row, fout, colIndex == this->columnCount-1);
                        row.clear();
                    }
                }
            }
        }
        else{ // efficiency hack
            Cursor cursor(this->matrixName, 0, true);
            vector<int> pageRow = cursor.getNext();
            vector<int> row;
            for(int rowIndex = 0; rowIndex < rowCount; rowIndex++){
                for(int colIndex = 0; colIndex < this->columnCount; colIndex++){
                    int rowCol = (rowIndex * this->columnCount) + colIndex;
                    if(pageRow.size() > 0 && rowCol == pageRow[0]){
                        row.push_back(pageRow[1]);
                        pageRow = cursor.getNext();
                    }
                    else{
                        row.push_back(0);
                    }
                    if(colIndex == this->columnCount-1 || row.size() == SPARSE_BLOCK_SIZE) // 1000 integers can be stored in 8kB
                    {
                        this->writeRow(row, fout, colIndex == this->columnCount-1);
                        row.clear();
                    }
                }
            }
        }
    }
    else{
        Cursor cursor(this->matrixName, 0, 0, true);
        vector<int> row;
        for(int rowIndex = 0; rowIndex < rowCount; rowIndex++){
            for(int colBlockIndex = 0; colBlockIndex < this->colBlocks; colBlockIndex++){
                row = cursor.getNext(); // gets a row from page on pageIndex
                this->writeRow(row, fout, colBlockIndex == this->colBlocks-1);
            }
        }
    }
}

/**
 * @brief Function prints the first few rows of the matrix. If the matrix contains
 * more rows than PRINT_COUNT, exactly PRINT_COUNT rows are printed, else all
 * the rows are printed.
 *
 */
void Matrix::print() 
{
    logger.log("Matrix::print");
    uint count = min((long long)PRINT_COUNT, this->rowCount);
    
    this->printToSource(cout, count);

    cout << "\nRow Count: " << count << "\nColumn Count: " << this->columnCount << endl;
}

/**
 * @brief This function returns one row of the matrix using the cursor object. It
 * returns an empty row is all rows have been read.
 * It is used for sparse matrix
 *
 * @param cursor 
 * @return vector<int> 
 */
void Matrix::getNextPage(Cursor* cursor)
{
    logger.log("Matrix::getNext");

    if (cursor->pageIndex < this->blockCount - 1) 
    {
        cursor->nextPage(cursor->pageIndex + 1, true);
    }
}

/**
 * @brief called when EXPORT command is invoked to move source file to "data"
 * folder.
 *
 */
void Matrix::makePermanent() // SP: For EXPORTMAT: Implement
{
    logger.log("Matrix::makePermanent");
    if (!this->isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
    string newSourceFile = "../data/" + this->matrixName + ".csv";
    ofstream fout(newSourceFile, ios::out);

    this->printToSource(fout, this->rowCount);

    fout.close();
}

/**
 * @brief Function to check if matrix is already exported
 *
 * @return true if exported
 * @return false otherwise
 */
bool Matrix::isPermanent()
{
    logger.log("Matrix::isPermanent");
    if (this->sourceFileName == "../data/" + this->matrixName + ".csv")
        return true;
    return false;
}

/**
 * @brief The unload function removes the matrix from the database by deleting
 * all temporary files created as part of this matrix
 *
 */
void Matrix::unload()
{
    logger.log("Matrix::~unload");
    if (this->isSparse){
        for(int pageBlock = 0; pageBlock < this->blockCount; pageBlock++)
            bufferManager.deleteFile(this->matrixName, pageBlock);
    }
    else{
        for (int pageRowBlock = 0; pageRowBlock < this->rowBlocks; pageRowBlock++)
            for (int pageColBlock = 0; pageColBlock < this->colBlocks; pageColBlock++)
                bufferManager.deleteFile(this->matrixName, pageRowBlock, pageColBlock);
    }
    if (!isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
}

/**
 * @brief Function that returns a cursor that reads rows from this matrix
 * 
 * @return Cursor 
 */
Cursor Matrix::getCursor()
{
    logger.log("Matrix::getCursor");
    if(this->isSparse){
        Cursor cursor(this->matrixName, 0, true);
        return cursor;
    }
    else{
        Cursor cursor(this->matrixName, 0, 0, true);
        return cursor;
    }
}
