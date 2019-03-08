using ExpressBase.Common;
using ExpressBase.Common.Data;
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
        public DbClientServices(IEbConnectionFactory _dbf) : base(_dbf) { }
        EbDbExplorerTablesDict Table = new EbDbExplorerTablesDict();
        List<object> Row = new List<object>();

        public GetDbTablesResponse Get(GetDbTablesRequest request)
        {
            string sql = @"
                SELECT Q1.table_name, Q1.table_schema, i.indexname FROM 
                (SELECT
                    table_name, table_schema
                FROM
                    information_schema.tables s
                WHERE
                    table_schema != 'pg_catalog'
                    AND table_schema != 'information_schema'
                    AND table_type='BASE TABLE')Q1
                LEFT JOIN
                    pg_indexes i
                ON
                   Q1.table_name = i.tablename ORDER BY tablename;

                SELECT 
                    table_name, column_name, data_type
                FROM
                    information_schema.columns
                WHERE
                    table_schema != 'pg_catalog' AND
                    table_schema != 'information_schema'
                ORDER BY table_name;

               SELECT
                   c.conname AS constraint_name,
                   c.contype AS constraint_type,
                   tbl.relname AS tabless,
                   ARRAY_AGG(col.attname
                   ORDER BY
                   u.attposition)
                   AS columns,
                   pg_get_constraintdef(c.oid) AS definition
               FROM 
                    pg_constraint c
               JOIN 
                    LATERAL UNNEST(c.conkey) WITH
                    ORDINALITY AS u(attnum, attposition) ON TRUE
               JOIN 
                    pg_class tbl ON tbl.oid = c.conrelid
               JOIN 
                    pg_namespace sch ON sch.oid = tbl.relnamespace
               JOIN 
                    pg_attribute col ON(col.attrelid = tbl.oid AND col.attnum = u.attnum)
               GROUP BY 
                    constraint_name, constraint_type, tabless, definition
               ORDER BY 
                    tabless;
             ";

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
                    EbDbExplorerTable tab = new EbDbExplorerTable()
                    {
                        Name = Row[0].ToString(),
                        Schema = Row[1].ToString()
                    };
                    Table.TableCollection.Add(Row[0].ToString(), tab);
                    Table.TableCollection[Row[0].ToString()].Index.Add(Row[2].ToString());
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
                Table.TableCollection[Row[0].ToString()].Columns.Add(col);
            }
            Data = dt.Tables[2];
            foreach (var Row in Data.Rows)
            {
                ArrayList column = new ArrayList
                    {
                        Row[3]
                    };
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
                            string definition = Row[4].ToString();
                            string[] df = definition.Split("REFERENCES ");
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
                        }
                    }
                }
            }

            string DB_Name = this.EbConnectionFactory.ObjectsDB.DBName;
            return new GetDbTablesResponse
            {
                Tables = Table,
                DB_Name = DB_Name
            };
        }

        [CompressResponse]
        public DbClientQueryResponse Post(DbClientSelectRequest request)
        {
            EbDataSet _dataset = null;
            string mess = "SUCCESS";
            try
            {
                _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(request.Query, new System.Data.Common.DbParameter[0]);
            }
            catch (Exception e)
            {
                mess = e.Message;
            }
            return new DbClientQueryResponse { Dataset = _dataset, Message = mess };
        }

        [CompressResponse]
        public DbClientQueryResponse Post(DbClientInsertRequest request)
        {
            int res = 0;
            string mess = "SUCCESS";
            try
            {
                res = this.EbConnectionFactory.ObjectsDB.InsertTable(request.Query, new System.Data.Common.DbParameter[0]);
            }
            catch (Exception e)
            {
                mess = e.Message;
            }
            return new DbClientQueryResponse { Result = res, Type = DBOperations.INSERT, Message = mess };
        }

        [CompressResponse]
        public DbClientQueryResponse Post(DbClientDeleteRequest request)
        {
            int res = 0;
            string mess = "SUCCESS";
            try
            {
                res = this.EbConnectionFactory.ObjectsDB.DeleteTable(request.Query, new System.Data.Common.DbParameter[0]);
            }
            catch (Exception e)
            {
                mess = e.Message;
            }
            return new DbClientQueryResponse { Result = res, Type = DBOperations.DELETE, Message = mess };
        }

        [CompressResponse]
        public DbClientQueryResponse Post(DbClientDropRequest request)
        {
            var _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(request.Query, new System.Data.Common.DbParameter[0]);
            return new DbClientQueryResponse { Dataset = _dataset };
        }

        [CompressResponse]
        public DbClientQueryResponse Post(DbClientTruncateRequest request)
        {
            var _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(request.Query, new System.Data.Common.DbParameter[0]);
            return new DbClientQueryResponse { Dataset = _dataset };
        }

        [CompressResponse]
        public DbClientQueryResponse Post(DbClientUpdateRequest request)
        {
            int res = 0;
            string mess = "SUCCESS";
            try
            {
                res = this.EbConnectionFactory.ObjectsDB.UpdateTable(request.Query, new System.Data.Common.DbParameter[0]);
            }
            catch (Exception e)
            {
                mess = e.Message;
            }

            return new DbClientQueryResponse { Result = res, Type = DBOperations.UPDATE, Message = mess };
        }

        [CompressResponse]
        public DbClientQueryResponse Post(DbClientAlterRequest request)
        {
            int res = 0;
            string mess = "SUCCESS";
            try
            {
                res = this.EbConnectionFactory.ObjectsDB.AlterTable(request.Query, new System.Data.Common.DbParameter[0]);
            }
            catch (Exception e)
            {
                mess = e.Message;
            }
            return new DbClientQueryResponse { Result = res, Type = DBOperations.UPDATE, Message = mess };
        }

        [CompressResponse]
        public DbClientQueryResponse Post(DbClientCreateRequest request)
        {
            int res = 0;
            string mess = "SUCCESS";
            try
            {
                res = this.EbConnectionFactory.ObjectsDB.CreateTable(request.Query);
            }
            catch (Exception e)
            {
                mess = e.Message;
            }

            return new DbClientQueryResponse { Result = res, Type = DBOperations.CREATE, Message = mess };

        }

    }
}
