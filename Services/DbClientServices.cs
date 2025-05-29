﻿using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Data;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using System;
using System.Collections;
using System.Collections.Generic;
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
            string FuntionQuery = @"select n.nspname as function_schema, p.proname as function_name, pg_get_functiondef(p.oid) as Functions
                                        from pg_proc p left
                                        join pg_namespace n on p.pronamespace = n.oid
                                        where n.nspname not in ('pg_catalog', 'information_schema')
                                        order by function_schema, function_name; ";
            try
            {
                string sql = this.EbConnectionFactory.DataDB.EB_GETDBCLIENTTTABLES;
                sql = sql + FuntionQuery;
                if (request.IsAdminOwn || request.SupportLogin)
                {
                    if (request.IsAdminOwn)
                    {
                        string sql1 = "SELECT isolution_id FROM eb_solutions WHERE eb_del ='F';";
                        EbDataTable solutionTable = this.InfraConnectionFactory.DataDB.DoQuery(sql1);
                        foreach (var Row in solutionTable.Rows)
                        {
                            solutions.Add(Row[0].ToString());
                        }
                    }
                    sql = string.Format(sql, "");
                }
                else
                    sql = string.Format(sql, "eb_%");
                EbDataSet dt = this.EbConnectionFactory.DataDB.DoQueries(sql);
                var Data = dt.Tables[0];
                foreach (var Row in Data.Rows)
                {

                    if (Table.TableCollection.ContainsKey(Row[0].ToString()))
                    {
                        // dataItems.Clear();
                        //tab.Index.Add(dataReader[2].ToString());
                        Table.TableCollection[Row[0].ToString()].Index.Add(Row[2].ToString());
                        //continue;
                    }
                    else
                    {
                        //if(Row[0].ToString().IndexOf("eb_ ", StringComparison.OrdinalIgnoreCase) == 0)
                        {
                            EbDbExplorerTable tab = new EbDbExplorerTable()
                            {
                                Name = Row[0].ToString(),
                                Schema = Row[1].ToString()
                            };
                            Table.TableCollection.Add(Row[0].ToString(), tab);
                            Table.TableCollection[Row[0].ToString()].Index.Add(Row[2].ToString());
                            TableCount++;
                        }
                    }
                }
                Data = dt.Tables[1];
                foreach (var Row in Data.Rows)
                {
                    EbDbExplorerColumn col = new EbDbExplorerColumn()
                    {
                        ColumnName = Row[1].ToString(),
                        ColumnType = Row[2].ToString()
                    };
                    if (Table.TableCollection.ContainsKey(Row[0].ToString()))
                        Table.TableCollection[Row[0].ToString()].Columns.Add(col);
                }
                Data = dt.Tables[2];
                foreach (var Row in Data.Rows)
                {
                    ArrayList column;

                    if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.MYSQL)
                    {
                        string s = Row[3].ToString();
                        column = new ArrayList { s.Split(',') };
                    }
                    else
                    {
                        column = new ArrayList { Row[3] };
                    }
                    var t = Row[3];
                    var col = column[0];
                    var x = (col as string[])[0];
                    string constName = Row[0].ToString();
                    string[] st = constName.Split("_");
                    string _name = st[st.Length - 1];
                    if (_name.Equals("pkey"))
                    {
                        foreach (EbDbExplorerColumn obj in Table.TableCollection[Row[2].ToString()].Columns)
                        {
                            if (obj.ColumnName.Equals(x))
                            {
                                obj.ColumnKey = "Primary key";
                                obj.ConstraintName = constName;  // <- add this
                            }
                        }
                    }
                    if (_name.Equals("fkey"))
                    {
                        foreach (EbDbExplorerColumn obj in Table.TableCollection[Row[2].ToString()].Columns)
                        {
                            if (obj.ColumnName.Equals(x))
                            {
                                obj.ColumnKey = "Foreign key";
                                obj.ConstraintName = constName;  // <- add this
                                string definition = Row[4].ToString();
                                string[] df = definition.Split("REFERENCES ");
                                df[1] = ":: " + df[1];
                                obj.ColumnTable = df[df.Length - 1];
                            }
                        }
                    }
                    if (_name.Equals("uniquekey"))
                    {
                        foreach (EbDbExplorerColumn obj in Table.TableCollection[Row[2].ToString()].Columns)
                        {
                            if (obj.ColumnName.Equals(x))
                            {
                                obj.ColumnKey = "Unique key";
                                obj.ConstraintName = constName;  // <- add this
                            }
                        }
                    }

                }
                Data = dt.Tables[3];
                foreach (var Row in Data.Rows)
                {
                    EbDbExplorerFunctions Fun = new EbDbExplorerFunctions()
                    {
                        FunctionName = Row[1].ToString(),
                        FunctionQuery = Row[2].ToString()
                    };
                    Table.FunctionCollection.Add(Fun);
                }
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
            return new GetDbTablesResponse
            {
                Tables = Table,
                DB_Name = DB_Name,
                TableCount = TableCount,
                SolutionCollection = solutions
            };
        }



        [CompressResponse]
        [Authenticate]
        public DbClientQueryResponse Post(DbClientSelectRequest request)
        {
            EbDataSet _dataset = null;
            string mess = "SUCCESS";
            try
            {
                _dataset = this.EbConnectionFactory.DataDB.DoQueries(request.Query, new System.Data.Common.DbParameter[0]);
            }
            catch (Exception e)
            {
                mess = e.Message;
            }
            return new DbClientQueryResponse { Dataset = _dataset, Message = mess };
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
                    query = $"ALTER TABLE \"{request.TableName}\" ADD CONSTRAINT \"{request.ConstraintName}\" UNIQUE (\"{request.ColumnName}\")";
                }
                else
                {
                    throw new ArgumentException("Invalid constraint type specified.");
                }

                // Execute the query
                res = factory.DataDB.CreateConstraint(query, new System.Data.Common.DbParameter[0]);
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
        public DbClientQueryResponse Post(DbClientInsertRequest request)
        {
            int res = 0;
            string mess = "SUCCESS";
            try
            {
                res = this.EbConnectionFactory.DataDB.InsertTable(request.Query, new System.Data.Common.DbParameter[0]);
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
            return new DbClientQueryResponse { Dataset = _dataset };
        }

        [CompressResponse]
        [Authenticate]
        public DbClientQueryResponse Post(DbClientTruncateRequest request)
        {
            var _dataset = this.EbConnectionFactory.DataDB.DoQueries(request.Query, new System.Data.Common.DbParameter[0]);
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
            }
            catch (Exception e)
            {
                mess = e.Message;
            }

            return new DbClientQueryResponse { Result = res, Type = DBOperations.CREATE, Message = mess };

        }

    }
}
