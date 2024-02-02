using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DMSLib
{
    public class SQLConverter
    {
        public static async Task<SqliteConnection> DMSFileToSQLAsync(DMSFile file, IProgress<float> progress, CancellationToken cancellationToken)
        {
            SQLitePCL.Batteries_V2.Init();
            var queryConnection = new SqliteConnection("Data Source=:memory:");
            queryConnection.Open();

            try
            {
                await Task.Run(() =>
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    /* Get total row count */
                    var totalRowCount = file.Tables.Select(t => t.Rows.Count).Sum();

                    /* build all tables */
                    foreach (var table in file.Tables.GroupBy(t => t.DBName).Select(g => g.First()))
                    {
                        var makeTable = queryConnection.CreateCommand();
                        makeTable.CommandText = GetTableDDL(table);
                        makeTable.ExecuteNonQuery();
                    }

                    var tableCount = queryConnection.CreateCommand();
                    tableCount.CommandText = "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name != 'android_metadata' AND name != 'sqlite_sequence';";
                    var count = tableCount.ExecuteScalar();
                    var rowsAdded = 0;
                    /* add all the rows */
                    foreach (var table in file.Tables)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var insertCmd = queryConnection.CreateCommand();
                        StringBuilder insertBuilder = new StringBuilder();
                        insertBuilder.Append($"INSERT INTO {table.DBName}(");
                        foreach (var field in table.Metadata.FieldMetadata)
                        {
                            insertBuilder.Append($"{field.FieldName},");
                        }
                        insertBuilder.Append("__rowHash");
                        insertBuilder.Append(") VALUES(");
                        for (var x = 1; x <= table.Columns.Count; x++)
                        {
                            insertBuilder.Append($"@{x},");
                        }
                        insertBuilder.Append($"@{table.Columns.Count + 1}");
                        insertBuilder.Append(");");

                        insertCmd.CommandText = insertBuilder.ToString();

                        foreach (var row in table.Rows)
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            if (insertCmd.Parameters.Count == 0)
                            {
                                for (var x = 1; x <= table.Columns.Count; x++)
                                {
                                    if (row.GetValue(x - 1) == null)
                                    {
                                        insertCmd.Parameters.AddWithValue($"@{x}", DBNull.Value);
                                    }
                                    else
                                    {
                                        insertCmd.Parameters.AddWithValue($"@{x}", row.GetValue(x - 1));
                                    }
                                }

                                insertCmd.Parameters.AddWithValue($"@{table.Columns.Count + 1}", row.RowHash);

                            }
                            else
                            {
                                /* params exist, just update their values */
                                for (var x = 0; x < insertCmd.Parameters.Count - 1; x++)
                                {
                                    if (row.GetValue(x) == null)
                                    {
                                        insertCmd.Parameters[x].Value = DBNull.Value;
                                    }
                                    else
                                    {
                                        insertCmd.Parameters[x].Value = row.GetValue(x);
                                    }
                                }
                                insertCmd.Parameters[insertCmd.Parameters.Count - 1].Value = row.RowHash;
                            }
                            insertCmd.Prepare();
                            try
                            {
                                insertCmd.ExecuteNonQuery();
                            }
                            catch (SqliteException ex)
                            {

                            }
                            rowsAdded += 1;
                            progress?.Report((rowsAdded / (float)totalRowCount) * 100);
                        }
                    }
                });

            }
            catch (OperationCanceledException canceled)
            {
                queryConnection.Close();
                return null;
            }

            return queryConnection;
        }
        public static async Task<SqliteConnection> DMSTableToSQLAsync(DMSTable table, IProgress<float> progress, CancellationToken cancellationToken)
        {
            SQLitePCL.Batteries_V2.Init();
            var queryConnection = new SqliteConnection("Data Source=:memory:");
            queryConnection.Open();

            try
            {
                await Task.Run(() =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    /* build the table */
                    var makeTable = queryConnection.CreateCommand();
                    makeTable.CommandText = GetTableDDL(table);
                    makeTable.ExecuteNonQuery();

                    /* add all the rows */

                    var insertCmd = queryConnection.CreateCommand();
                    StringBuilder insertBuilder = new StringBuilder();
                    insertBuilder.Append($"INSERT INTO {table.DBName}(");
                    foreach (var field in table.Metadata.FieldMetadata)
                    {
                        insertBuilder.Append($"{field.FieldName},");
                    }
                    insertBuilder.Append("__rowHash");
                    insertBuilder.Append(") VALUES(");
                    for (var x = 1; x <= table.Columns.Count; x++)
                    {
                        insertBuilder.Append($"@{x},");
                    }
                    insertBuilder.Append($"@{table.Columns.Count + 1}");
                    insertBuilder.Append(");");

                    insertCmd.CommandText = insertBuilder.ToString();
                    long rowsAdded = 0;
                    foreach (var row in table.Rows)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (insertCmd.Parameters.Count == 0)
                        {
                            for (var x = 1; x <= table.Columns.Count; x++)
                            {
                                if (row.GetValue(x - 1) == null)
                                {
                                    insertCmd.Parameters.AddWithValue($"@{x}", DBNull.Value);
                                }
                                else
                                {
                                    insertCmd.Parameters.AddWithValue($"@{x}", row.GetValue(x - 1));
                                }
                            }

                            insertCmd.Parameters.AddWithValue($"@{table.Columns.Count + 1}", row.RowHash);

                        }
                        else
                        {
                            /* params exist, just update their values */
                            for (var x = 0; x < insertCmd.Parameters.Count - 1; x++)
                            {
                                if (row.GetValue(x) == null)
                                {
                                    insertCmd.Parameters[x].Value = DBNull.Value;
                                }
                                else
                                {
                                    insertCmd.Parameters[x].Value = row.GetValue(x);
                                }
                            }
                            insertCmd.Parameters[insertCmd.Parameters.Count - 1].Value = row.RowHash;
                        }
                        insertCmd.Prepare();
                        try
                        {
                            insertCmd.ExecuteNonQuery();
                        }
                        catch (SqliteException ex)
                        {

                        }
                        rowsAdded += 1;
                        progress?.Report((rowsAdded / (float)table.Rows.Count) * 100);
                    }
                });
            }
            catch (OperationCanceledException canceled)
            {
                queryConnection.Close();
                return null;
            }

            return queryConnection;
        }


        public static async Task<SqliteDataReader> ExecuteQuery(SqliteConnection queryConnection, string query)
        {
            if (queryConnection == null)
            {
                throw new InvalidOperationException("queryConnection cannot be null.");
            }
            
            var queryCommand = queryConnection.CreateCommand();
            queryCommand.CommandText = query;

            return await queryCommand.ExecuteReaderAsync();
            
        }

        internal static string GetTableDDL(DMSTable table)
        {
            var x = table.Metadata.FieldMetadata[0];

            StringBuilder sb = new StringBuilder();
            sb.Append("CREATE TABLE ");
            sb.Append(table.DBName);
            sb.Append("(");
            foreach (var col in table.Metadata.FieldMetadata)
            {
                sb.Append($"{col.FieldName} {GetDataType(col)},");
            }
            sb.Append("__rowHash NUMBER");
            List<DMSRecordFieldMetadata> keys = table.Metadata.FieldMetadata.Where(m => m.UseEditMask.HasFlag(UseEditFlags.KEY)).ToList();
            if (keys.Count() > 0)
            {
                sb.Append(", PRIMARY KEY (");
                foreach(var key in keys)
                {
                    sb.Append(key.FieldName);
                    sb.Append(",");
                }
                sb.Length--;
                sb.Append(")");
            }
            sb.Append(");");
            return sb.ToString();
        }

        internal static string GetDataType(DMSRecordFieldMetadata fieldInfo)
        {

            var type = fieldInfo.FieldType;
            var decimals = fieldInfo.DecimalPositions;
            switch (type)
            {
                case FieldTypes.CHAR:
                    return "TEXT";
                case FieldTypes.LONG_CHAR:
                    return "TEXT";
                case FieldTypes.NUMBER:
                    if (decimals > 0)
                    {
                        return "REAL";
                    }
                    return "INTEGER";
                case FieldTypes.SIGNED_NUMBER:
                    if (decimals > 0)
                    {
                        return "REAL";
                    }
                    return "INTEGER";
                case FieldTypes.DATE:
                    return "TEXT";
                case FieldTypes.TIME:
                    return "TEXT";
                case FieldTypes.DATETIME:
                    return "TEXT";
                case FieldTypes.IMG_OR_ATTACH:
                    return "BLOB";
                case FieldTypes.IMAGE_REF:
                    return "BLOB";
                default:
                    Debugger.Break();
                    return "";
            }

        }

    }
}
