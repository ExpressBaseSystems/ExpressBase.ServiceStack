using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class ExcelUploadService : EbBaseService
    {
        public ExcelUploadService(IEbConnectionFactory _dbf) : base(_dbf) { }

        [CompressResponse]

        //.......check table name already exist.......
        public CheckTblResponse Any(CheckTblRequest request)
        {
            string qry = "SELECT EXISTS (SELECT 1 FROM   information_schema.tables WHERE  table_schema = 'public' AND table_name = @tbl); ";
            DbParameter[] parameter = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("@tbl", System.Data.DbType.String, request.tblName.ToLower()) };
            var rslt = this.EbConnectionFactory.ObjectsDB.DoQuery(qry, parameter);
            CheckTblResponse response = new CheckTblResponse();
            response.msg = rslt.Rows[0];
            return response;
        }

        //........create new table.......................
        public CreateTblResponse Any(CreateTblRequest request)
        {
            var x = request.headerList.abc.Count();
            string query = string.Format("CREATE TABLE IF NOT EXISTS public.{0}(", request.tblName);
            for (int i = 0; i < request.headerList.abc.Count(); i++)
            {
                query += string.Format(request.headerList.abc[i].colName + " " + request.headerList.abc[i].dataType + ",");
                //var t = dbtype(request.headerList.abc[i].dataType);
            }
            query = query.Remove(query.LastIndexOf(','));
            query += "); ";
            this.EbConnectionFactory.ObjectsDB.DoNonQuery(query);
            return new CreateTblResponse();
        }

        //.......Insert excel data into table............
        public InsertIntoTblResponse Any(InsertIntoTblResponseRequest request)
        {
            List<DbParameter> parameters = new List<DbParameter>();
            string sql = "";
            DataTable dt = JsonConvert.DeserializeObject<DataTable>(request.dtTbl);
            int iter2 = 1;

            Dictionary<string, string> dict = request.dataType;

            foreach (DataRow dr in dt.Rows.Cast<DataRow>().Skip(1))
            {
                sql += string.Format("INSERT INTO " + request.tblName + " (");


                foreach (DataColumn dc in dt.Columns)
                {
                    string col = dc.ColumnName.Replace(" ", "");
                    col = col.ToLower();

                    sql += string.Format(col + ",");

                }
                sql = sql.Remove(sql.LastIndexOf(','));
                sql += ") VALUES (";

                for (int i = 0; i < dt.Columns.Count; i++)
                {
                    var item = dr.ItemArray[i];

                    sql += "@item" + iter2 + ",";
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@item" + iter2, dbtype(dict[dt.Columns[i].ToString()]), item));
                    iter2++;
                }
                sql = sql.Remove(sql.LastIndexOf(','));
                sql += "); ";

            }
            this.EbConnectionFactory.ObjectsDB.DoNonQuery(sql, parameters.ToArray());
            return new InsertIntoTblResponse();
        }

        //..........method for retrive dbtype.........
        public DbType dbtype(string type)
        {
            DbType data_type = System.Data.DbType.String;
            switch (type.ToLower())
            {
                case "text":
                case "string":
                case "character":
                    data_type = System.Data.DbType.String;
                    break;
                case "ansistring":
                    data_type = System.Data.DbType.AnsiString;
                    break;
                case "binary":
                    data_type = System.Data.DbType.Binary;
                    break;
                case "byte":
                    data_type = System.Data.DbType.Byte;
                    break;
                case "boolean":
                    data_type = System.Data.DbType.Boolean;
                    break;
                case "currency":
                    data_type = System.Data.DbType.Currency;
                    break;
                case "date":
                    data_type = System.Data.DbType.Date;
                    break;
                case "timestamp":
                case "datetime":
                    data_type = System.Data.DbType.DateTime;
                    break;
                case "decimal":
                    data_type = System.Data.DbType.Decimal;
                    break;
                case "numeric":
                case "double":
                    data_type = System.Data.DbType.Double;
                    break;
                case "guid":
                    data_type = System.Data.DbType.Guid;
                    break;
                case "smallint":
                case "int16":
                    data_type = System.Data.DbType.Int16;
                    break;
                case "int32":
                    data_type = System.Data.DbType.Int32;
                    break;
                case "integer":
                case "int64":
                    data_type = System.Data.DbType.Int64;
                    break;
                case "object":
                    data_type = System.Data.DbType.Object;
                    break;
                case "sbyte":
                    data_type = System.Data.DbType.SByte;
                    break;
                case "single":
                    data_type = System.Data.DbType.Single;
                    break;
                case "time":
                    data_type = System.Data.DbType.Time;
                    break;
                case "uint16":
                    data_type = System.Data.DbType.UInt16;
                    break;
                case "uint32":
                    data_type = System.Data.DbType.UInt32;
                    break;
                case "uint64":
                    data_type = System.Data.DbType.UInt64;
                    break;
                case "varnumeric":
                    data_type = System.Data.DbType.VarNumeric;
                    break;
                case "ansistringfixedlength":
                    data_type = System.Data.DbType.AnsiStringFixedLength;
                    break;
                case "stringfixedlength":
                    data_type = System.Data.DbType.StringFixedLength;
                    break;
                case "xml":
                    data_type = System.Data.DbType.Xml;
                    break;
                case "datetime2":
                    data_type = System.Data.DbType.DateTime2;
                    break;
                case "datetimeoffset":
                    data_type = System.Data.DbType.DateTimeOffset;
                    break;
            }
            return data_type;
        }

        //.......create new table and insert excel data
        public ExcelCreateTableResponse Any(ExcelCreateTableRequest request)
        {
            DataTable dt = JsonConvert.DeserializeObject<DataTable>(request.DataTbl);


            ExcelCreateTableResponse resp = new ExcelCreateTableResponse();
            string query1 = string.Format("CREATE TABLE IF NOT EXISTS public.{0}(", request.tbl);
            string query2 = "";


            foreach (DataColumn dc in dt.Columns)
            {
                query1 += string.Format("{0} TEXT, ", dc.ColumnName);

            }
            query1 = query1.Remove(query1.LastIndexOf(','));
            query1 += "); ";



            foreach (DataRow dr in dt.Rows.Cast<DataRow>().Skip(1))
            {
                query2 += string.Format("INSERT INTO " + request.tbl + " (");
                foreach (DataColumn dc in dt.Columns)
                {
                    string col = dc.ColumnName.Replace(" ", "");
                    col = col.ToLower();
                    query2 += string.Format(col + ",");
                }
                query2 = query2.Remove(query2.LastIndexOf(','));
                query2 += ") VALUES (";
                foreach (object item in dr.ItemArray)
                {
                    var s = item.ToString();
                    query2 += string.Format("'" + item.ToString() + "',");
                }
                query2 = query2.Remove(query2.LastIndexOf(','));
                query2 += "); ";
            }
            string query = query1 + query2;


            this.EbConnectionFactory.ObjectsDB.DoNonQuery(query);


            return new ExcelCreateTableResponse();
        }

        //........Get Tables............
        public DBTableResponse Get(DBTableRequest request)
        {
            string qry = string.Format("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema' ORDER BY tablename asc;");

            var rslt = this.EbConnectionFactory.ObjectsDB.DoQuery(qry);

            List<String> tbl_name = new List<string>();
            foreach (EbDataRow dr in rslt.Rows)
            {
                tbl_name.Add(dr[0].ToString());
            }
            int i = tbl_name.Count();

            DBTableResponse response = new DBTableResponse();
            response.list1 = tbl_name;

            return response;
        }

        //.......Get Columns......................
        public DBColumnResponse Any(DBColumnRequest request)
        {
            string qry = string.Format("select column_name,data_type from INFORMATION_SCHEMA.COLUMNS where table_name='") + request.tblName + string.Format("';");

            //DataTable rslt = new DataTable();
            var rslt = this.EbConnectionFactory.ObjectsDB.DoQuery(qry);

            return new DBColumnResponse { tbl = rslt };
        }
    }
}

