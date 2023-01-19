namespace BatchLoadTest
{
    using Microsoft.Data.SqlClient;
    using Microsoft.VisualBasic.FileIO;

    internal class Program
    {
        
        // test the following batch sizes
        static int[] testBatchSizes = new int[] { 32768, 16384, 8192, 4096, 2048, 1024, 512, 256, 128, 64, 32, 16, 8, 4, 2, 1 };
        // data file to load
        //const string dataFileName = "..\\..\\..\\Data\\PLACES__Local_Data_for_Better_Health__Census_Tract_Data_2022_release.csv";
        const string dataFileName = "..\\..\\..\\Data\\Small Test Data File.csv";

        // database connection
        const string dbConnStr = "Data Source=localhost;Initial Catalog=BatchLoadTest;Integrated Security=True;Pooling=False";
        const string tempTableName = "Bob1234";
        const int maxCols = 200;                    // Max number of columns to load
        const int maxColWidth = 1024;               // Max size for each column

        // Create the temporary table, dropping it first if it exists, to load data to
        private static void CreateTempTable()
        {
            using (SqlConnection sqlConn = new SqlConnection(dbConnStr))
            {
                string columnClause = "";

                for (int i = 0; i < maxCols; i++)
                {
                    columnClause += $"[Column{i}] VARCHAR({maxColWidth}), ";
                }
                // remove the trailing comma
                columnClause = columnClause.Remove(columnClause.Length - 2);

                SqlCommand cmdDropTable = new SqlCommand($"IF OBJECT_ID (N'{tempTableName}', N'U') IS NOT NULL DROP TABLE {tempTableName};", sqlConn);
                SqlCommand cmdCreateTable = new SqlCommand($"CREATE TABLE {tempTableName} (RowID INT IDENTITY(1,1), {columnClause});", sqlConn);

                try
                {
                    sqlConn.Open();
                    cmdDropTable.ExecuteNonQuery();
                    cmdCreateTable.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        // Load the data file into the temp Table using a specified batch size
        private static int LoadData(string srcDataFileName, int batchSize)
        {
            // Buffer for sql command batch
            string[] cmdBatch = new string[batchSize];
            int currentBatchRecordNbr = 0;
            int rowCount = 0;

            using (TextFieldParser csvReader = new TextFieldParser(srcDataFileName))
            {
                csvReader.Delimiters = new string[] { "," };

                while (!csvReader.EndOfData)
                {
                    String colInsertClause = "";
                    String colNames = "";

                    rowCount++;

                    string[] cols = csvReader.ReadFields();
                    int colNbr = 0;
                    foreach (string col in cols)
                    {
                        colNames += $"[Column{colNbr}], ";
                        colNbr++;

                        if (col.Length == 0)
                        {
                            colInsertClause += "NULL, ";
                        }
                        else
                        {
                            string cleanStr = col.Replace("'", "''");
                            colInsertClause += $"'{cleanStr}', ";
                        }
                    }
                    // remove the trailing comma and space
                    colInsertClause = colInsertClause.Remove(colInsertClause.Length - 2);
                    colNames = colNames.Remove(colNames.Length - 2);

                    String insertCmdText = $"INSERT INTO {tempTableName} ({colNames}) VALUES ({colInsertClause});";

                    // Add the insert to the batch
                    cmdBatch[currentBatchRecordNbr++] = insertCmdText;

                    // if we've reached the batch size then execute the batch.
                    if (currentBatchRecordNbr >= batchSize)
                    {
                        ExecuteInsertBatch(cmdBatch);

                        // Empty the batch
                        currentBatchRecordNbr = 0;
                        cmdBatch = new string[batchSize];
                    }
                }
            
                // If there are any commands in the batch flush the batch
                if (currentBatchRecordNbr > 0 )
                {
                    ExecuteInsertBatch(cmdBatch);
                }

                return rowCount;
            }

            static void ExecuteInsertBatch(string[] cmdBatch)
            {
                using (SqlConnection sqlConn = new SqlConnection(dbConnStr))
                {
                    // append the batch together and wrap it in a single transaction
                    string sqlCmd = "begin transaction\n" + String.Join("\n", cmdBatch) + "\ncommit";
                    
                    SqlCommand cmdInsert = new SqlCommand(sqlCmd, sqlConn);

                    try
                    {
                        sqlConn.Open();
                        cmdInsert.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
        }
            static void Main(string[] args)
        {
            Console.WriteLine("\"BatchSize\", \"Execution Time (s)\", \"Records Loaded\", \"Records Per Second\"");

            foreach (int batchSize in testBatchSizes) 
            {
                CreateTempTable();
                DateTime startTime = DateTime.Now;
                int recordsLoaded = LoadData(dataFileName, batchSize);
                DateTime endTime = DateTime.Now;
                TimeSpan executionTime = endTime - startTime;
                Console.WriteLine($"{batchSize}, {executionTime.TotalSeconds}, {recordsLoaded}, {recordsLoaded / (executionTime.TotalSeconds > 0 ? executionTime.TotalSeconds : 1)}");
            }
        }
    }
}



