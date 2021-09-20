#include "executor.h"

extern float BLOCK_SIZE;
extern uint BLOCK_COUNT;
extern uint PRINT_COUNT;
extern uint BLOCK_ROW_COUNT;
extern uint BLOCK_COL_COUNT;
extern vector<string> tokenizedQuery;
extern ParsedQuery parsedQuery;
extern TableCatalogue tableCatalogue;
extern MatrixCatalogue matrixCatalogue;
extern BufferManager bufferManager;