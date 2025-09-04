using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Data;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class DbClientServices : EbBaseService
    {
        public DbClientServices(IEbConnectionFactory _dbf, IEbMqClient _mq) : base(_dbf, _mq) { }

        [Authenticate]
        public EbConnectionFactory GetFactory(bool IsAdminOwn, string ClientSolnid)
        {
            EbConnectionFactory factory = null;
            try
            {
                if (IsAdminOwn && ClientSolnid != null)
                {
                    RefreshSolutionConnectionsAsyncResponse resp = new RefreshSolutionConnectionsAsyncResponse();
                    //EbConnectionsConfig conf = EbConnectionsConfigProvider.GetDataCenterConnections();
                    //conf.DataDbConfig.DatabaseName = ClientSolnid;
                    factory = new EbConnectionFactory(ClientSolnid, this.Redis);
                    resp = this.MQClient.Post<RefreshSolutionConnectionsAsyncResponse>(new RefreshSolutionConnectionsBySolutionIdAsyncRequest()
                    {
                        SolutionId = ClientSolnid
                    });
                }
                else
                    factory = this.EbConnectionFactory;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return factory;
        }

        EbDbExplorerTablesDict Table = new EbDbExplorerTablesDict();
        List<object> Row = new List<object>();
        List<string> solutions = new List<string>();
        [Authenticate]
        public GetDbTablesResponse Get(GetDbTablesRequest request)
        {
            string DB_Name = "";
            int TableCount = 0;

            // Query to get PostgreSQL functions (excluding system schemas)
            string FuntionQuery = @"
        SELECT 
            n.nspname AS function_schema, 
            p.proname AS function_name, 
            pg_get_functiondef(p.oid) AS Functions
        FROM pg_proc p 
        LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY function_schema, function_name;";

            try
            {
                // Step 1: Get base SQL from service constant
                string sql = this.EbConnectionFactory.DataDB.EB_GETDBCLIENTTTABLES;

                // Step 2: Append the function query to main SQL (result will be in dt.Tables[3])
                sql += FuntionQuery;

                // Step 3: Handle solution filtering for admin or support login
                if (request.IsAdminOwn || request.SupportLogin)
                {
                    if (request.IsAdminOwn)
                    {
                        // Get all active solutions
                        string sql1 = "SELECT isolution_id FROM eb_solutions WHERE eb_del = 'F';";
                        EbDataTable solutionTable = this.InfraConnectionFactory.DataDB.DoQuery(sql1);

                        foreach (var Row in solutionTable.Rows)
                        {
                            solutions.Add(Row[0].ToString());
                        }
                    }

                    sql = string.Format(sql, ""); // No filter
                }
                else
                {
                    // Apply filter for eb_ tables only
                    sql = string.Format(sql, "eb_%");
                }

                // Step 4: Execute combined query
                EbDataSet dt = this.EbConnectionFactory.DataDB.DoQueries(sql);

                // ------------------ TABLES + INDEXES ------------------
                var Data = dt.Tables[0];

                foreach (var Row in Data.Rows)
                {
                    string tableName = Row[0].ToString();

                    if (Table.TableCollection.ContainsKey(tableName))
                    {
                        Table.TableCollection[tableName].Index.Add(Row[2].ToString());
                    }
                    else
                    {
                        var tab = new EbDbExplorerTable
                        {
                            Name = tableName,
                            Schema = Row[1].ToString()
                        };
                        Table.TableCollection.Add(tableName, tab);
                        Table.TableCollection[tableName].Index.Add(Row[2].ToString());
                        TableCount++;
                    }
                }

                // ------------------ COLUMNS ------------------
                Data = dt.Tables[1];
                foreach (var Row in Data.Rows)
                {
                    EbDbExplorerColumn col = new EbDbExplorerColumn
                    {
                        ColumnName = Row[1].ToString(),
                        ColumnType = Row[2].ToString()
                    };

                    string tableName = Row[0].ToString();
                    if (Table.TableCollection.ContainsKey(tableName))
                    {
                        Table.TableCollection[tableName].Columns.Add(col);
                    }
                }

                // ------------------ CONSTRAINTS ------------------
                Data = dt.Tables[2];
                foreach (var Row in Data.Rows)
                {
                    string constName = Row[0].ToString();
                    string[] columns = ((string[])((Array)Row[3])).ToArray();
                    string definition = Row[4].ToString();
                    string lowerConstName = constName.ToLower();
                    string tableName = Row[2].ToString();

                    foreach (string colName in columns)
                    {
                        if (!Table.TableCollection.ContainsKey(tableName))
                            continue;

                        var columnObj = Table.TableCollection[tableName].Columns
                            .FirstOrDefault(c => c.ColumnName == colName);

                        if (columnObj == null) continue;

                        columnObj.ConstraintName = constName;

                        if (lowerConstName.Contains("pkey"))
                        {
                            columnObj.ColumnKey = "Primary key";
                        }
                        else if (lowerConstName.Contains("fkey"))
                        {
                            columnObj.ColumnKey = "Foreign key";

                            string[] df = definition.Split("REFERENCES ");
                            if (df.Length > 1)
                            {
                                columnObj.ColumnTable = ":: " + df[1];
                            }
                        }
                        else if (lowerConstName.Contains("unique_key"))
                        {
                            columnObj.ColumnKey = "Unique key";
                        }
                    }
                }

                // ------------------ FUNCTIONS ------------------
                Data = dt.Tables[3];
                foreach (var Row in Data.Rows)
                {
                    EbDbExplorerFunctions Fun = new EbDbExplorerFunctions
                    {
                        FunctionName = Row[1].ToString(),
                        FunctionQuery = Row[2].ToString(),
                    };
                    Table.FunctionCollection.Add(Fun);
                }

                // Save the DB name for reference
                DB_Name = this.EbConnectionFactory.ObjectsDB.DBName;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
                return new GetDbTablesResponse
                {
                    Message = e.Message
                };
            }

            // Final output
            return new GetDbTablesResponse
            {
                Tables = Table,
                DB_Name = DB_Name,
                TableCount = TableCount,
                SolutionCollection = solutions
            };
        }

        private void LogQuery(string queryText, int queryType, int rowsAffected, string solutionId, int? createdByUserId)
        {
            try
            {
                string logQuery = @"
        INSERT INTO eb_dbclient_logs 
        (query, type, rows_affected, solution_id, eb_created_by, eb_created_at, eb_del) 
        VALUES 
        (@query, @type, @rows_affected, @solution_id, @eb_created_by, NOW(), 'F')";

                DbParameter[] parameters = new DbParameter[]
                {
            this.EbConnectionFactory.DataDB.GetNewParameter("@query", EbDbTypes.String, queryText ?? string.Empty),
            this.EbConnectionFactory.DataDB.GetNewParameter("@type", EbDbTypes.Int32, queryType),
            this.EbConnectionFactory.DataDB.GetNewParameter("@rows_affected", EbDbTypes.Int32, rowsAffected),
            this.EbConnectionFactory.DataDB.GetNewParameter("@solution_id", EbDbTypes.String, solutionId ?? string.Empty),
            this.EbConnectionFactory.DataDB.GetNewParameter("@eb_created_by", EbDbTypes.Int32, createdByUserId ?? 0)
                };

                // Debug check before execution
                foreach (var p in parameters)
                {
                    Console.WriteLine($"{p.ParameterName} = {p.Value ?? "NULL"}");
                }

                this.EbConnectionFactory.DataDB.DoQuery(logQuery, parameters);
            }
            catch (Exception ex)
            {
                Console.WriteLine("LogQuery failed: " + ex.Message);
            }
        }


        [CompressResponse]
        [Authenticate]
        public DbClientQueryResponse Post(DbClientSelectRequest request)
        {
            EbDataSet _dataset = null;
            string mess = "SUCCESS";

            // ✅ Step 1: Whitelisted eb_ tables
            var allowedEbTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "eb_files_ref", "eb_files_ref_variations", "eb_role2location", "eb_role2permission",
        "eb_role2role", "eb_roles", "eb_signin_log", "eb_user2usergroup",
        "eb_usersanonymous", "eb_usergroup", "eb_users", "eb_userstatus",
        "eb_public_holidays", "eb_notifications", "eb_user_types", "eb_my_actions",
        "eb_approval_lines", "eb_sms_logs", "eb_browser_exceptions", "eb_email_logs",
        "eb_downloads", "eb_fin_years", "eb_fin_years_lines", "eb_files_bytea",
        "eb_executionlogs", "eb_locations", "eb_location_types"
    };

            try
            {
                string queryLower = request.Query.ToLower();

                // ✅ Step 2: Find all eb_ tables used in the query
                var ebTableMatches = System.Text.RegularExpressions.Regex.Matches(
                    queryLower,
                    @"\beb_[a-zA-Z0-9_]+\b"
                );

                // ✅ Step 3: Check for disallowed eb_ tables
                foreach (System.Text.RegularExpressions.Match match in ebTableMatches)
                {
                    var tableName = match.Value;
                    if (!allowedEbTables.Contains(tableName))
                    {
                        throw new Exception($"Access to table '{tableName}' is not allowed.");
                    }
                }

                // ✅ Step 4: Execute query only if all eb_ tables are allowed
                _dataset = this.EbConnectionFactory.DataDB.DoQueries(request.Query, new System.Data.Common.DbParameter[0]);
            }
            catch (Exception e)
            {
                mess = e.Message;
            }

            return new DbClientQueryResponse
            {
                Dataset = _dataset,
                Message = mess
            };
        }
        [CompressResponse]
        [Authenticate]
        public List<DbClientLogsResponse> Post(DbClientLogsRequest request)
        {
            List<DbClientLogsResponse> logs = new List<DbClientLogsResponse>();

            try
            {
                EbConnectionFactory factory = GetFactory(request.IsAdminOwn, request.SolutionId);

                string query = @"
        SELECT 
            l.id,
            l.query,
            l.type,
            CASE 
                WHEN l.rows_affected = -1 THEN 'N/A'
                WHEN l.type = 1 THEN l.rows_affected || ' row(s) inserted'
                WHEN l.type = 2 THEN l.rows_affected || ' row(s) updated'
                WHEN l.type = 3 THEN l.rows_affected || ' row(s) deleted'
                ELSE l.rows_affected || ' row(s)'
            END AS rows_result,
            l.solution_id,
            u.fullname AS created_by_name,
            l.eb_created_at
        FROM eb_dbclient_logs l
        LEFT JOIN eb_users u ON u.id = l.eb_created_by
        WHERE l.eb_del = 'F'
        ";

                List<DbParameter> parameters = new List<DbParameter>();

                if (!string.IsNullOrEmpty(request.SolutionId))
                {
                    query += " AND l.solution_id = @solutionId";
                    parameters.Add(factory.DataDB.GetNewParameter("@solutionId", EbDbTypes.String, request.SolutionId));
                }

                if (!string.IsNullOrEmpty(request.TableName))
                {
                    query += " AND l.query LIKE @tableName";
                    parameters.Add(factory.DataDB.GetNewParameter("@tableName", EbDbTypes.String, "%" + request.TableName + "%"));
                }

                query += " ORDER BY l.eb_created_at DESC LIMIT 500";

                // Debug logging to see final query & parameters
                Console.WriteLine("==== DB CLIENT LOGS QUERY ====");
                Console.WriteLine(query);
                foreach (var p in parameters)
                {
                    Console.WriteLine($"Param: {p.ParameterName} = {p.Value}");
                }
                Console.WriteLine("================================");

                EbDataTable dbResults = factory.DataDB.DoQuery(query, parameters.ToArray());

                foreach (var row in dbResults.Rows)
                {
                    logs.Add(new DbClientLogsResponse
                    {
                        Id = Convert.ToInt32(row["id"]),
                        Query = row["query"]?.ToString(),
                        Type = Convert.ToInt32(row["type"]),
                        RowsResult = row["rows_result"]?.ToString(),
                        SolutionId = row["solution_id"]?.ToString(),
                        CreatedByName = row["created_by_name"]?.ToString(),
                        CreatedAt = Convert.ToDateTime(row["eb_created_at"])
                        // ResponseMessage removed (column doesn't exist)
                    });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to fetch logs: " + ex.Message);
            }

            return logs;
        }




        [CompressResponse]
        [Authenticate]
        public DbClientIndexResponse Post(DbClientIndexRequest request)
        {
            int res = 0;
            string mess = "SUCCESSFULLY CREATED INDEX";

            try
            {
                EbConnectionFactory factory = GetFactory(request.IsAdminOwn, request.ClientSolnid);
                string query = $"CREATE INDEX {request.IndexName} ON {request.TableName} ({request.IndexColumns})";

                res = factory.DataDB.CreateIndex(query, new System.Data.Common.DbParameter[0]);

                // Log query with CREATE_INDEX type
                LogQuery(query, (int)DBOperations.CREATE_INDEX, res, request.ClientSolnid, request.CreatedByUserId);
            }
            catch (Exception e)
            {
                mess = e.Message;
            }

            return new DbClientIndexResponse { Result = res, Type = DBOperations.CREATE_INDEX, Message = mess };
        }




        [CompressResponse]
        [Authenticate]
        public DbClientConstraintResponse Post(DbClientConstraintRequest request)
        {
            int res = 0;
            string mess = "Constraint created successfully";
            string query = "";

            try
            {
                EbConnectionFactory factory = GetFactory(request.IsAdminOwn, request.ClientSolnid);

                // Validate inputs
                if (string.IsNullOrEmpty(request.TableName) || string.IsNullOrEmpty(request.ColumnName) || string.IsNullOrEmpty(request.ConstraintType) || string.IsNullOrEmpty(request.ConstraintName))
                {
                    throw new ArgumentException("Table name, column name, constraint type, and constraint name must be provided.");
                }

                // Determine the type of constraint to create
                if (string.Equals(request.ConstraintType, "Primary Key", StringComparison.OrdinalIgnoreCase))
                {
                    query = $"ALTER TABLE \"{request.TableName}\" ADD CONSTRAINT \"{request.ConstraintName}\" PRIMARY KEY (\"{request.ColumnName}\")";
                }
                else if (string.Equals(request.ConstraintType, "Unique Key", StringComparison.OrdinalIgnoreCase))
                {
                    // Split by comma, trim spaces, and wrap each column in double quotes
                    string[] columns = request.ColumnName.Split(',')
                        .Select(c => $"\"{c.Trim()}\"")
                        .ToArray();

                    string joinedColumns = string.Join(", ", columns);

                    query = $"ALTER TABLE \"{request.TableName}\" ADD CONSTRAINT \"{request.ConstraintName}\" UNIQUE ({joinedColumns})";
                }
                else
                {
                    throw new ArgumentException("Invalid constraint type specified.");
                }

                // Execute the query
                res = factory.DataDB.CreateConstraint(query, new System.Data.Common.DbParameter[0]);
                LogQuery(query, (int)DBOperations.CREATE_CONSTRAINT, res, request.ClientSolnid, request.CreatedByUserId);

            }
            catch (Exception e)
            {
                res = -1;
                mess = "Error: " + e.Message;
                if (e.InnerException != null)
                {
                    mess += " | Inner Exception: " + e.InnerException.Message;
                }
            }

            return new DbClientConstraintResponse
            {
                Result = res,
                Type = DBOperations.CREATE_CONSTRAINT,
                Message = mess
            };
        }



        [CompressResponse]
        [Authenticate]
        public DbClientCreateFunctionResponse Post(DbClientCreateFunctionRequest request)
        {
            int result = 0;
            string message = "Function created successfully";
            string query = "";

            try
            {
                // Validate inputs
                if (string.IsNullOrEmpty(request.FunctionName) || string.IsNullOrEmpty(request.FunctionCode))
                {
                    throw new ArgumentException("Function name and function code must be provided.");
                }

                // Get the factory for DB access
                EbConnectionFactory factory = GetFactory(request.IsAdminOwn, request.ClientSolnid);

                // Extract the function SQL code
                query = request.FunctionCode;

                // Execute the query
                result = factory.DataDB.CreateFunction(query, new System.Data.Common.DbParameter[0]);
                LogQuery(query, (int)DBOperations.CREATE_FUNCTION, result, request.ClientSolnid, request.CreatedByUserId);


            }
            catch (Exception ex)
            {
                result = -1;
                message = "Error: " + ex.Message;
                if (ex.InnerException != null)
                {
                    message += " | Inner Exception: " + ex.InnerException.Message;
                }
            }

            return new DbClientCreateFunctionResponse
            {
                Result = result,
                Message = message,
                FunctionName = request.FunctionName,
                Type = DBOperations.CREATE_FUNCTION
            };
        }

        [CompressResponse]
        [Authenticate]
        public DbClientLogEditedFunctionResponse Post(DbClientLogEditedFunctionRequest request)
        {
            int res = 0;
            string mess = "Function edit logged successfully";

            try
            {
                EbConnectionFactory factory = GetFactory(request.IsAdminOwn, request.ClientSolnid);

                if (string.IsNullOrEmpty(request.FunctionName) || string.IsNullOrEmpty(request.FunctionCode))
                {
                    throw new ArgumentException("Function name and function code must be provided.");
                }

                // Build log text
                string logText = $"-- Edited Function Log\n-- Function: {request.FunctionName}\n{request.FunctionCode}";

                // Call LogQuery (like in CreateConstraint)
                LogQuery(logText, (int)DBOperations.ALTER, 0, request.ClientSolnid, request.CreatedByUserId);
            }
            catch (Exception ex)
            {
                res = -1;
                mess = "Error: " + ex.Message;
            }

            return new DbClientLogEditedFunctionResponse
            {
                Result = res,
                Type = DBOperations.ALTER,
                Message = mess
            };
        }

        [CompressResponse]
        [Authenticate]
        public List<DbClientFunctionHistoryResponse> Post(DbClientFunctionHistoryRequest request)
        {
            List<DbClientFunctionHistoryResponse> logs = new List<DbClientFunctionHistoryResponse>();

            try
            {
                EbConnectionFactory factory = GetFactory(request.IsAdminOwn, request.SolutionId);

                string query = @"
SELECT 
    l.id,
    l.query AS function_code,
    l.solution_id,
    u.fullname AS created_by_name,
    l.eb_created_at
FROM eb_dbclient_logs l
LEFT JOIN eb_users u ON u.id = l.eb_created_by
WHERE l.eb_del = 'F' AND l.type = @type
";

                List<DbParameter> parameters = new List<DbParameter>
        {
            factory.DataDB.GetNewParameter("@type", EbDbTypes.Int32, (int)DBOperations.ALTER) // make sure this matches your enum
        };

                if (!string.IsNullOrEmpty(request.SolutionId))
                {
                    query += " AND l.solution_id = @solutionId";
                    parameters.Add(factory.DataDB.GetNewParameter("@solutionId", EbDbTypes.String, request.SolutionId));
                }

                if (!string.IsNullOrEmpty(request.FunctionName))
                {
                    query += " AND l.query LIKE @functionName";
                    parameters.Add(factory.DataDB.GetNewParameter("@functionName", EbDbTypes.String, "%" + request.FunctionName + "%"));
                }

                query += " ORDER BY l.eb_created_at DESC LIMIT 500";

                EbDataTable dbResults = factory.DataDB.DoQuery(query, parameters.ToArray());

                foreach (var row in dbResults.Rows)
                {
                    logs.Add(new DbClientFunctionHistoryResponse
                    {
                        Id = Convert.ToInt32(row["id"]),
                        FunctionName = request.FunctionName,
                        FunctionCode = row["function_code"]?.ToString(),
                        SolutionId = row["solution_id"]?.ToString(),
                        CreatedByName = row["created_by_name"]?.ToString(),
                        CreatedAt = Convert.ToDateTime(row["eb_created_at"]),
                    });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("GetFunctionHistory failed: " + ex.Message);
            }

            return logs;
        }



        [CompressResponse]
        [Authenticate]
        public DbClientQueryResponse Post(DbClientInsertRequest request)
        {
            int res = 0;
            string mess = "SUCCESS";

            try
            {
                // Debug log
                Console.WriteLine("Incoming SQL:");
                Console.WriteLine(request.Query);

                string q = request.Query?.Trim() ?? "";

                // Must start with INSERT
                if (!q.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase))
                    throw new ArgumentException("Invalid or empty INSERT query.");

                // No empty column list
                if (q.Contains("() VALUES"))
                    throw new ArgumentException("INSERT statement has no columns defined.");

                // No empty VALUES
                if (q.Contains("VALUES ()"))
                    throw new ArgumentException("INSERT statement has no values provided.");

                // No trailing comma before closing parenthesis
                if (System.Text.RegularExpressions.Regex.IsMatch(q, @",\s*\)"))
                    throw new ArgumentException("INSERT column list has an extra comma.");

                // Run insert
                res = this.EbConnectionFactory.DataDB.InsertTable(q, new System.Data.Common.DbParameter[0]);

                // Log query
                LogQuery(q, (int)DBOperations.INSERT, res, request.ClientSolnid, request.CreatedByUserId);
            }
            catch (Exception e)
            {
                mess = e.Message;
            }

            return new DbClientQueryResponse { Result = res, Type = DBOperations.INSERT, Message = mess };
        }


        [CompressResponse]
        [Authenticate]
        public DbClientQueryResponse Post(DbClientDeleteRequest request)
        {
            int res = 0;
            string mess = "SUCCESS";
            try
            {
                res = this.EbConnectionFactory.DataDB.DeleteTable(request.Query, new System.Data.Common.DbParameter[0]);
                LogQuery(request.Query, (int)DBOperations.DELETE, res, request.ClientSolnid, request.CreatedByUserId);

            }
            catch (Exception e)
            {
                mess = e.Message;
            }
            return new DbClientQueryResponse { Result = res, Type = DBOperations.DELETE, Message = mess };
        }

        [CompressResponse]
        [Authenticate]
        public DbClientQueryResponse Post(DbClientDropRequest request)
        {
            var _dataset = this.EbConnectionFactory.DataDB.DoQueries(request.Query, new System.Data.Common.DbParameter[0]);
            LogQuery(request.Query, (int)DBOperations.DROP, 0, request.ClientSolnid, request.CreatedByUserId);

            return new DbClientQueryResponse { Dataset = _dataset };
        }

        [CompressResponse]
        [Authenticate]
        public DbClientQueryResponse Post(DbClientTruncateRequest request)
        {
            var _dataset = this.EbConnectionFactory.DataDB.DoQueries(request.Query, new System.Data.Common.DbParameter[0]);
            LogQuery(request.Query, (int)DBOperations.TRUNCATE, 0, request.ClientSolnid, request.CreatedByUserId);
            return new DbClientQueryResponse { Dataset = _dataset };
        }

        [CompressResponse]
        [Authenticate]
        public DbClientQueryResponse Post(DbClientUpdateRequest request)
        {
            int res = 0;
            string mess = "SUCCESS";
            try
            {
                res = this.EbConnectionFactory.DataDB.UpdateTable(request.Query, new System.Data.Common.DbParameter[0]);
                LogQuery(request.Query, (int)DBOperations.UPDATE, res, request.ClientSolnid, request.CreatedByUserId);

            }
            catch (Exception e)
            {
                mess = e.Message;
            }

            return new DbClientQueryResponse { Result = res, Type = DBOperations.UPDATE, Message = mess };
        }

        [CompressResponse]
        [Authenticate]
        public DbClientQueryResponse Post(DbClientAlterRequest request)
        {
            int res = 0;
            string mess = "SUCCESS";
            try
            {
                res = this.EbConnectionFactory.DataDB.AlterTable(request.Query, new System.Data.Common.DbParameter[0]);
                // Log ALTER operation
                LogQuery(request.Query, (int)DBOperations.ALTER, res, request.ClientSolnid, request.CreatedByUserId);

            }
            catch (Exception e)
            {
                mess = e.Message;
            }
            return new DbClientQueryResponse { Result = res, Type = DBOperations.UPDATE, Message = mess };
        }

        [CompressResponse]
        [Authenticate]
        public DbClientQueryResponse Post(DbClientCreateRequest request)
        {
            int res = 0;
            string mess = "SUCCESS";
            try
            {
                res = this.EbConnectionFactory.DataDB.CreateTable(request.Query);
                LogQuery(request.Query, (int)DBOperations.CREATE, res, request.ClientSolnid, request.CreatedByUserId);

            }
            catch (Exception e)
            {
                mess = e.Message;
            }

            return new DbClientQueryResponse { Result = res, Type = DBOperations.CREATE, Message = mess };

        }

    }
}
