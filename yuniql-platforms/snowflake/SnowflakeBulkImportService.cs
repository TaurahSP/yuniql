using Snowflake.Data.Client;
using System.Data;
using System.IO;
using Yuniql.Extensibility;

//https://github.com/22222/CsvTextFieldParser
namespace Yuniql.Snowflake
{
    public class SnowflakeBulkImportService : IBulkImportService
    {
        private string _connectionString;
        private readonly ITraceService _traceService;

        public SnowflakeBulkImportService(ITraceService traceService)
        {
            this._traceService = traceService;
        }

        public void Initialize(string connectionString)
        {
            this._connectionString = connectionString;
        }

        public void Run(
            IDbConnection connection,
            IDbTransaction transaction,
            string fileFullPath,
            string delimiter = null,
            int? batchSize = null,
            int? commandTimeout = null)
        {
            //check if a non-default dbo schema is used
            var schemaName = "PUBLIC";
            var tableName = Path.GetFileNameWithoutExtension(fileFullPath);
            if (tableName.IndexOf('.') > 0)
            {
                schemaName = tableName.Split('.')[0];
                tableName = tableName.Split('.')[1];
            }

            //read csv file and load into data table
            var dataTable = ParseCsvFile(fileFullPath, delimiter);

            //save the csv data into staging sql table
            BulkCopyWithDataTable(connection, transaction, tableName, dataTable);
        }

        private DataTable ParseCsvFile(
            string csvFileFullPath,
            string delimiter = null)
        {
            if (string.IsNullOrEmpty(delimiter))
                delimiter = ",";

            var csvDatatable = new DataTable();
            using (var csvReader = new CsvTextFieldParser(csvFileFullPath))
            {
                csvReader.Delimiters = (new string[] { delimiter });
                csvReader.HasFieldsEnclosedInQuotes = true;

                string[] csvColumns = csvReader.ReadFields();
                foreach (string csvColumn in csvColumns)
                {
                    var dataColumn = new DataColumn(csvColumn);
                    dataColumn.AllowDBNull = true;
                    csvDatatable.Columns.Add(dataColumn);
                }

                while (!csvReader.EndOfData)
                {
                    string[] fieldData = csvReader.ReadFields();
                    for (int i = 0; i < fieldData.Length; i++)
                    {
                        if (fieldData[i] == "" || fieldData[i] == "NULL")
                        {
                            fieldData[i] = null;
                        }
                    }
                    csvDatatable.Rows.Add(fieldData);
                }
            }
            return csvDatatable;
        }

        //NOTE: This is not the most typesafe and performant way to do this and this is just to demonstrate
        //possibility to bulk import data in custom means during migration execution
        private void BulkCopyWithDataTable(
            IDbConnection connection,
            IDbTransaction transaction,
            string tableName,
            DataTable dataTable)
        {
            _traceService.Info($"SnowflakeImportService: Started copying data into destination table {tableName}");

            using (var cmd = new SnowflakeDbCommand())
            {
                cmd.Connection = connection as SnowflakeDbConnection;
                cmd.Transaction = transaction as SnowflakeDbTransaction;
                cmd.CommandText = $"SELECT * FROM {tableName} LIMIT 0;";

                using (var adapter = new SnowflakeDbDataAdapter(cmd))
                {
                    adapter.UpdateBatchSize = 10000;
                    using (var cb = new SnowflakeDbCommandBuilder(adapter))
                    {
                        cb.SetAllValues = true;
                        adapter.Update(dataTable);
                    }
                };

                _traceService.Info($"SnowflakeImportService: Finished copying data into destination table {tableName}");
            }
        }

    }
}

