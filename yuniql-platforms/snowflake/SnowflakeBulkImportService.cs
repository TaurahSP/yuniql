using Snowflake.Data.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
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

            _traceService.Info($"SnowflakeImportService: Started copying data into destination table {schemaName}.{tableName}");

            //read csv file and load into data table
            var sqlStatement = PrepareMultiRowInsertStatement(schemaName, tableName, fileFullPath, delimiter);
            using (var cmd = new SnowflakeDbCommand())
            {
                cmd.Connection = connection as SnowflakeDbConnection;
                cmd.Transaction = transaction as SnowflakeDbTransaction;
                cmd.CommandText = sqlStatement;
                cmd.ExecuteNonQuery();
            }

            _traceService.Info($"SnowflakeImportService: Finished copying data into destination table {schemaName}.{tableName}");
        }

        //NOTE: This is not the most typesafe and performant way to do this and this is just to demonstrate
        //possibility to bulk import data in custom means during migration execution
        private string PrepareMultiRowInsertStatement(
            string schemaName,
            string tableName,
            string csvFileFullPath,
            string delimiter = null)
        {
            var sqlStatement = new StringBuilder();

            if (string.IsNullOrEmpty(delimiter))
                delimiter = ",";

            using (var csvReader = new CsvTextFieldParser(csvFileFullPath))
            {
                csvReader.Delimiters = (new string[] { delimiter });
                csvReader.HasFieldsEnclosedInQuotes = true;

                string[] csvColumns = csvReader.ReadFields();
                sqlStatement.Append($"INSERT INTO \"{schemaName}\".\"{tableName}\" ({string.Join(",", csvColumns)}) {Environment.NewLine}");
                sqlStatement.AppendFormat("VALUES {0}", Environment.NewLine);

                while (!csvReader.EndOfData)
                {
                    var fieldData = csvReader.ReadFields().Select(s=> s.Quote());
                    sqlStatement.Append($"{Environment.NewLine}({string.Join(",", fieldData)}),");
                }
            }

            return sqlStatement
                .ToString().TrimEnd(',');
        }
    }
}

