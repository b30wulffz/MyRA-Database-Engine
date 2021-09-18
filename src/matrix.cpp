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
    return this->calculateStats() && this->blockify();
}

bool Matrix::calculateStats()
{
    ifstream fin(this->sourceFileName, ios::in);
    string line, value;
    long long nonZeroCount = 0;
    bool columnCnt = true;
    while (getline(fin, line))
    {

        stringstream rowStream(line);
        while (getline(rowStream, value, ','))
        {
            if (stoi(value) != 0)
                nonZeroCount++;
            if(columnCnt)
                this->columnCount++;
        }
        columnCnt = false;
        this->rowCount++;
    }
    fin.close();
    long long cellsCount = this->columnCount * this->rowCount;
    if(nonZeroCount <= (int)(0.4*cellsCount)) this->isSparse = true;
    return true;
}

/**
 * @brief This function splits all the rows and stores them in multiple files of
 * one block size. 
 *
 * @return true if successfully blockified
 * @return false otherwise
 */
bool Matrix::blockify() // SP: Load matrix in chunk 44*44 : Implement
{
    uint chunkCols = 2;
    uint chunkRows = 2;
    logger.log("Matrix::blockify");
                     // SP: So basically a row is being stored in a vector, assuming its values to be integer
    // vector<vector<int>> rowsInPage(chunkRows, row); // SP: This stores all such rows, which are to be written in a single block
    
    this->colBlocks = (uint)ceil((1.0*this->columnCount)/chunkCols);
    this->rowBlocks = (uint)ceil((1.0*this->rowCount)/chunkRows);

    long long seekLength = 0;

    ifstream fin(this->sourceFileName, ios::in);
    for(uint colBlockIndex=0; colBlockIndex < this->colBlocks; colBlockIndex++){
        string line, value;
        bool seekLengthFlag = true;
        long long currentSeek = seekLength;
        uint rowBlockIndex = 0;
        uint rowInBlock = 0;
        vector<vector<int>> block;
        while (getline(fin, line))
        {
            vector<int> row;
            stringstream rowStream(line);
            rowStream.seekg(currentSeek);
            while(getline(rowStream, value, ',')){
                if(seekLengthFlag) seekLength+=(value.size()+1LL);
                row.push_back(stoi(value));
                if(row.size() >= chunkCols) break;
            }
            block.push_back(row);
            seekLengthFlag = false;
            rowInBlock++;
            if(rowInBlock == chunkRows){
                bufferManager.writePage(this->matrixName, rowBlockIndex, colBlockIndex, block, rowInBlock);
                rowInBlock = 0;
                block.clear();
                rowBlockIndex++;
            }
        }
        if(rowInBlock){
            bufferManager.writePage(this->matrixName, rowBlockIndex, colBlockIndex, block, rowInBlock);
            block.clear();
        }
        fin.clear();
        fin.seekg(0, ios::beg);
    }
    fin.close();
    if (this->rowCount == 0)
        return false;
    return true;
}

/**
 * @brief Function prints the first few rows of the matrix. If the matrix contains
 * more rows than PRINT_COUNT, exactly PRINT_COUNT rows are printed, else all
 * the rows are printed.
 *
 */
void Matrix::print() // SP: For PrintMat: atmost 20 rows : implement
{
    logger.log("Matrix::print");
    uint count = min((long long)PRINT_COUNT, this->rowCount);

    //print headings
    // this->writeRow(this->columns, cout);

    Cursor cursor(this->matrixName, 0);
    vector<int> row;
    for (int rowCounter = 0; rowCounter < count; rowCounter++)
    {
        row = cursor.getNext(); // gets a row from page on pageIndex
        this->writeRow(row, cout);
    }
    printRowCount(this->rowCount);
}

/**
 * @brief This function returns one row of the matrix using the cursor object. It
 * returns an empty row is all rows have been read.
 *
 * @param cursor 
 * @return vector<int> 
 */
void Matrix::getNextPage(Cursor *cursor)
{
    logger.log("Matrix::getNext");

    if (cursor->pageIndex < this->blockCount - 1) // SP: If pages have pageindex under the limit
    {
        cursor->nextPage(cursor->pageIndex + 1);
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

    //print headings
    // this->writeRow(this->columns, fout);

    Cursor cursor(this->matrixName, 0);
    vector<int> row;
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        row = cursor.getNext();
        this->writeRow(row, fout);
    }
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
    for (int pageRowBlock = 0; pageRowBlock < this->rowBlocks; pageRowBlock++)
        for (int pageColBlock = 0; pageColBlock < this->colBlocks; pageColBlock++)
            bufferManager.deleteFile(this->matrixName, pageRowBlock, pageColBlock);
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
    Cursor cursor(this->matrixName, 0);
    return cursor;
}
