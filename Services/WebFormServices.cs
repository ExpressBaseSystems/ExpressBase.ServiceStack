using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class WebFormServices : EbBaseService
    {
        public WebFormServices(IEbConnectionFactory _dbf) : base(_dbf) { }

        //========================================== FORM TABLE CREATION  ==========================================

        public CreateWebFormTableResponse Any(CreateWebFormTableRequest request)
        {
            if (request.WebObj is EbWebForm)
                (request.WebObj as EbWebForm).AfterRedisGet(this);

            //CreateWebFormTableRec(request.WebObj, request.WebObj.TableName);

            WebFormSchema _temp = GetWebFormSchema(request.WebObj);/////////
            CreateWebFormTables(_temp);

            return new CreateWebFormTableResponse { };
        }

        private WebFormSchema GetWebFormSchema(EbControlContainer _container)
        {
            WebFormSchema _formSchema = new WebFormSchema();
            _formSchema.FormName = _container.Name;
            _formSchema.MasterTable = _container.TableName.ToLower();
            _formSchema.Tables = new List<TableSchema>();
            _formSchema = GetWebFormSchemaRec(_formSchema, _container);
            return _formSchema;
        }

        private WebFormSchema GetWebFormSchemaRec(WebFormSchema _schema, EbControlContainer _container)
        {
            IEnumerable<EbControl> _flatControls = _container.Controls.Get1stLvlControls();
            TableSchema _table = _schema.Tables.FirstOrDefault(tbl => tbl.TableName == _container.TableName);
            if (_table == null)
            {
                List<ColumSchema> _columns = new List<ColumSchema>();
                foreach (EbControl control in _flatControls)
                {
                    _columns.Add(new ColumSchema { ColumName = control.Name, EbDbType = (int)control.EbDbType });
                }
                if(_columns.Count > 0)
                    _schema.Tables.Add(new TableSchema { TableName = _container.TableName.ToLower(), Colums = _columns });
            }
            else
            {
                foreach (EbControl control in _flatControls)
                {
                    _table.Colums.Add(new ColumSchema { ColumName = control.Name, EbDbType = (int)control.EbDbType });
                }
            }
            foreach (EbControl _control in _container.Controls)
            {
                if (_control is EbControlContainer)
                {
                    EbControlContainer Container = _control as EbControlContainer;

                    if (Container.TableName.IsNullOrEmpty())
                    {
                        Container.TableName = _container.TableName;
                    }
                    _schema = GetWebFormSchemaRec(_schema, Container);
                }
            }
            return _schema;
        }

        private void CreateWebFormTables(WebFormSchema _schema)
        {
            IVendorDbTypes vDbTypes = this.EbConnectionFactory.ObjectsDB.VendorDbTypes;
            foreach (TableSchema _table in _schema.Tables)
            {
                List<TableColumnMeta> _listNamesAndTypes = new List<TableColumnMeta>();
                if(_table.Colums.Count > 0)
                {
                    foreach (ColumSchema _column in _table.Colums)
                    {
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = _column.ColumName, Type = vDbTypes.GetVendorDbTypeStruct((EbDbTypes)_column.EbDbType) });
                    }
                    if(_table.TableName != _schema.MasterTable)
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = _table + "_id", Type = vDbTypes.Decimal });// id refernce to the parent table will store in this column - foreignkey
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_by", Type = vDbTypes.Decimal });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_at", Type = vDbTypes.DateTime });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_by", Type = vDbTypes.Decimal });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_at", Type = vDbTypes.DateTime });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_del", Type = vDbTypes.Boolean, Default = "F" });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_void", Type = vDbTypes.Boolean, Default = "F" });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_transaction_date", Type = vDbTypes.DateTime });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_autogen", Type = vDbTypes.Decimal });

                    CreateOrAlterTable(_table.TableName, _listNamesAndTypes);
                }                
            }
        }

        //private void CreateWebFormTableRec(EbControlContainer _container, string _table)
        //{
        //    CreateWebFormTableHelper(_container, _table);
        //    foreach (EbControl _control in _container.Controls)
        //    {
        //        if (_control is EbControlContainer)
        //        {
        //            EbControlContainer Container = _control as EbControlContainer;

        //            if (Container.TableName.IsNullOrEmpty())
        //            {
        //                Container.TableName = _container.TableName;
        //            }
        //            CreateWebFormTableRec(Container, _container.TableName);
        //        }
        //    }
        //}

        //private void CreateWebFormTableHelper(EbControlContainer _container, string _table)
        //{
        //    IVendorDbTypes vDbTypes = this.EbConnectionFactory.ObjectsDB.VendorDbTypes;
        //    List<TableColumnMeta> _listNamesAndTypes = new List<TableColumnMeta>();
        //    IEnumerable<EbControl> _flatControls = _container.Controls.Get1stLvlControls();

        //    foreach (EbControl control in _flatControls)
        //    {
        //        _listNamesAndTypes.Add(new TableColumnMeta { Name = control.Name, Type = control.GetvDbType(vDbTypes) });
        //    }
        //    if (_listNamesAndTypes.Count > 0)
        //    {
        //        if (!_table.ToLower().Equals(_container.TableName.ToLower()))
        //            _listNamesAndTypes.Add(new TableColumnMeta { Name = _table + "_id", Type = vDbTypes.Decimal });// id refernce to the parent table will store in this column - foreignkey
        //        _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_by", Type = vDbTypes.Decimal });
        //        _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_at", Type = vDbTypes.DateTime });
        //        _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_by", Type = vDbTypes.Decimal });
        //        _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_at", Type = vDbTypes.DateTime });
        //        _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_del", Type = vDbTypes.Boolean, Default = "F" });
        //        _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_void", Type = vDbTypes.Boolean, Default = "F" });
        //        _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_transaction_date", Type = vDbTypes.DateTime });
        //        _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_autogen", Type = vDbTypes.Decimal });

        //        CreateOrAlterTable(_container.TableName.ToLower(), _listNamesAndTypes);
        //    }
        //}

        private int CreateOrAlterTable(string tableName, List<TableColumnMeta> listNamesAndTypes)
        {
            //checking for space in column name, table name
            foreach (TableColumnMeta entry in listNamesAndTypes)
            {
                if (entry.Name.Contains(CharConstants.SPACE) || tableName.Contains(CharConstants.SPACE))
                    return -1;
            }
            var isTableExists = this.EbConnectionFactory.ObjectsDB.IsTableExists(this.EbConnectionFactory.ObjectsDB.IS_TABLE_EXIST, new DbParameter[] { this.EbConnectionFactory.ObjectsDB.GetNewParameter("tbl", EbDbTypes.String, tableName) });
            if (!isTableExists)
            {
                string cols = string.Join(CharConstants.COMMA + CharConstants.SPACE.ToString(), listNamesAndTypes.Select(x => x.Name + CharConstants.SPACE + x.Type.VDbType.ToString() + (x.Default.IsNullOrEmpty() ? "" : (" DEFAULT '" + x.Default + "'"))).ToArray());
                string sql = string.Empty;
                if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.ORACLE)////////////
                {
                    sql = "CREATE TABLE @tbl(id NUMBER(10), @cols)".Replace("@cols", cols).Replace("@tbl", tableName);
                    this.EbConnectionFactory.ObjectsDB.CreateTable(sql);//Table Creation
                    CreateSquenceAndTrigger(tableName);//
                }
                else
                {
                    sql = "CREATE TABLE @tbl( id SERIAL PRIMARY KEY, @cols)".Replace("@cols", cols).Replace("@tbl", tableName);
                    this.EbConnectionFactory.ObjectsDB.CreateTable(sql);
                }
                return 0;
            }
            else
            {
                var colSchema = this.EbConnectionFactory.ObjectsDB.GetColumnSchema(tableName);
                string sql = string.Empty;
                foreach (TableColumnMeta entry in listNamesAndTypes)
                {
                    bool isFound = false;
                    foreach (EbDataColumn dr in colSchema)
                    {
                        if (entry.Name.ToLower() == (dr.ColumnName.ToLower()))
                        {
                            isFound = true;
                            break;
                        }
                    }
                    if (!isFound)
                    {
                        sql += entry.Name + " " + entry.Type.VDbType.ToString() + " " + (entry.Default.IsNullOrEmpty() ? "" : (" DEFAULT '" + entry.Default + "'")) + ",";
                    }
                }
                bool appendId = false;
                var existingIdCol = colSchema.FirstOrDefault(o => o.ColumnName.ToLower() == "id");
                if (existingIdCol == null)
                    appendId = true;
                if (!sql.IsEmpty() || appendId)
                {
                    if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.ORACLE)/////////////////////////
                    {
                        sql = (appendId ? "id NUMBER(10)," : "") + sql;
                        if (!sql.IsEmpty())
                        {
                            sql = "ALTER TABLE @tbl ADD (" + sql.Substring(0, sql.Length - 1) + ")";
                            sql = sql.Replace("@tbl", tableName);
                            this.EbConnectionFactory.ObjectsDB.UpdateTable(sql);
                            if (appendId)
                                CreateSquenceAndTrigger(tableName);
                        }
                    }
                    else
                    {
                        sql = (appendId ? "id SERIAL," : "") + sql;
                        if (!sql.IsEmpty())
                        {
                            sql = "ALTER TABLE @tbl ADD COLUMN " + (sql.Substring(0, sql.Length - 1)).Replace(",", ", ADD COLUMN ");
                            sql = sql.Replace("@tbl", tableName);
                            this.EbConnectionFactory.ObjectsDB.UpdateTable(sql);
                        }
                    }
                    return (0);
                }
            }
            return -1;
        }

        private void CreateSquenceAndTrigger(string tableName)
        {
            string sqnceSql = "CREATE SEQUENCE @name_sequence".Replace("@name", tableName);
            string trgrSql = string.Format(@"CREATE OR REPLACE TRIGGER {0}_on_insert
													BEFORE INSERT ON {0}
													FOR EACH ROW
													BEGIN
														SELECT {0}_sequence.nextval INTO :new.id FROM dual;
													END;", tableName);
            this.EbConnectionFactory.ObjectsDB.CreateTable(sqnceSql);//Sequence Creation
            this.EbConnectionFactory.ObjectsDB.CreateTable(trgrSql);//Trigger Creation
        }


        //================================== GET RECORD FOR RENDERING ================================================

        public GetRowDataResponse Any(GetRowDataRequest request)
        {            
            GetRowDataResponse _dataset = new GetRowDataResponse();
            _dataset.FormData = GetWebformData(request.RefId, request.RowId);
            return _dataset;
        }

        private string GetSelectQuery(WebFormSchema _schema)
        {
            string query = string.Empty;

            foreach(TableSchema _table in _schema.Tables)
            {
                string _cols = string.Empty;
                string _id = "id";
                if(_table.Colums.Count > 0)
                {
                    foreach (ColumSchema _column in _table.Colums)
                    {
                        _cols += "," + _column.ColumName;
                    }
                    if (_table.TableName != _schema.MasterTable)
                        _id = _schema.MasterTable + "_id";
                    query += string.Format("SELECT id {0} FROM {1} WHERE {2} = :id;", _cols, _table.TableName, _id);
                }                
            }
            
            return query;
        }

        private WebformData GetWebformData(string _refId, int _rowid)
        {
            EbWebForm FormObj = GetWebFormObject(_refId);
            WebFormSchema _schema = GetWebFormSchema(FormObj);
            string query = GetSelectQuery(_schema);

            EbDataSet dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(query, new DbParameter[] { this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, _rowid) });

            WebformData FormData = new WebformData();

            foreach (EbDataTable dataTable in dataset.Tables)
            {
                SingleTable Table = new SingleTable();
                foreach (EbDataRow dataRow in dataTable.Rows)
                {
                    SingleRow Row = new SingleRow();
                    foreach (EbDataColumn dataColumn in dataTable.Columns)
                    {
                        object _unformattedData = dataRow[dataColumn.ColumnIndex];
                        object _formattedData = _unformattedData;

                        if (dataColumn.Type == EbDbTypes.Date)
                        {
                            _unformattedData = (_unformattedData == DBNull.Value) ? DateTime.MinValue : _unformattedData;
                            _formattedData = ((DateTime)_unformattedData).Date != DateTime.MinValue ? Convert.ToDateTime(_unformattedData).ToString("yyyy-MM-dd") : string.Empty;
                        }
                        Row.Columns.Add(new SingleColumn()
                        {
                            Name = dataColumn.ColumnName,
                            Type = (int)dataColumn.Type,
                            Value = _formattedData
                        });
                    }
                    Row.RowId = dataRow[dataTable.Columns[0].ColumnIndex].ToString();
                    Table.Add(Row);
                }
                if (!FormData.MultipleTables.ContainsKey(dataTable.TableName))
                    FormData.MultipleTables.Add(dataTable.TableName, Table);
            }
            FormData.MasterTable = dataset.Tables[0].TableName;
            return FormData;
        }

        //private WebformData getDataSetAsRowCollection(string _refid, int _rowid)
        //{
        //    EbWebForm FormObj = GetWebFormObject(_refid);
        //    FormObj.TableRowId = _rowid;
        //    string query = FormObj.GetSelectQuery(FormObj.TableName);
        //    EbDataSet dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(query);

        //    WebformData FormData = new WebformData();

        //    foreach (EbDataTable dataTable in dataset.Tables)
        //    {
        //        SingleTable Table = new SingleTable();
        //        foreach (EbDataRow dataRow in dataTable.Rows)
        //        {
        //            SingleRow Row = new SingleRow();
        //            foreach (EbDataColumn dataColumn in dataTable.Columns)
        //            {
        //                object _unformattedData = dataRow[dataColumn.ColumnIndex];
        //                object _formattedData = _unformattedData;

        //                if (dataColumn.Type == EbDbTypes.Date)
        //                {
        //                    _unformattedData = (_unformattedData == DBNull.Value) ? DateTime.MinValue : _unformattedData;
        //                    _formattedData = ((DateTime)_unformattedData).Date != DateTime.MinValue ? Convert.ToDateTime(_unformattedData).ToString("yyyy-MM-dd") : string.Empty;
        //                }
        //                Row.Columns.Add(new SingleColumn() {
        //                    Name = dataColumn.ColumnName,
        //                    Type = (int)dataColumn.Type,
        //                    Value = _formattedData
        //                });
        //            }
        //            Row.RowId = dataRow[dataTable.Columns[0].ColumnIndex].ToString();
        //            Table.Add(Row);
        //        }
        //        if (!FormData.MultipleTables.ContainsKey(dataTable.TableName))
        //            FormData.MultipleTables.Add(dataTable.TableName, Table);
        //    }
        //    FormData.MasterTable = dataset.Tables[0].TableName;
        //    return FormData;
        //}

        //private List<object> getDataSetAsRowCollection(EbDataSet dataset)
        //{
        //    List<object> rowColl = new List<object>();
        //    foreach (EbDataTable dataTable in dataset.Tables)
        //    {
        //        foreach (EbDataRow dataRow in dataTable.Rows)
        //        {
        //            foreach (EbDataColumn dataColumn in dataTable.Columns)
        //            {
        //                object _unformattedData = dataRow[dataColumn.ColumnIndex];
        //                object _formattedData = _unformattedData;

        //                if (dataColumn.Type == EbDbTypes.Date)
        //                {
        //                    _unformattedData = (_unformattedData == DBNull.Value) ? DateTime.MinValue : _unformattedData;
        //                    _formattedData = ((DateTime)_unformattedData).Date != DateTime.MinValue ? Convert.ToDateTime(_unformattedData).ToString("yyyy-MM-dd") : string.Empty;
        //                }
        //                rowColl.Add(_formattedData);
        //            }
        //        }
        //    }
        //    return rowColl;
        //}

        private EbWebForm GetWebFormObject(string RefId)
        {
            EbWebForm _form = this.Redis.Get<EbWebForm>(RefId);
            if(_form == null)
            {
                var myService = base.ResolveService<EbObjectService>();
                EbObjectParticularVersionResponse formObj = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = RefId });
                _form = EbSerializers.Json_Deserialize(formObj.Data[0].Json);
                this.Redis.Set<EbWebForm>(RefId, _form);
            }            
            _form.AfterRedisGet(this);
            return _form;
        }

        public DoUniqueCheckResponse Any(DoUniqueCheckRequest Req)
        {
            string query = string.Format("SELECT id FROM {0} WHERE {1} = :value;", Req.TableName, Req.Field);
            EbControl obj = Activator.CreateInstance(typeof(ExpressBase.Objects.Margin).Assembly.GetType("ExpressBase.Objects." + Req.TypeS, true), true) as EbControl;
            DbParameter[] param = {
                this.EbConnectionFactory.DataDB.GetNewParameter("value",obj.EbDbType, Req.Value)
            };
            EbDataTable datatbl = this.EbConnectionFactory.ObjectsDB.DoQuery(query, param);
            return new DoUniqueCheckResponse { NoRowsWithSameValue = datatbl.Rows.Count };
        }

        public GetDictionaryValueResponse Any(GetDictionaryValueRequest request)
        {
            Dictionary<string, string> Dict = new Dictionary<string, string>();
            string qry = @"
SELECT 
	k.key, v.value
FROM 
	eb_keys k, eb_languages l, eb_keyvalue v
WHERE
	k.id = v.key_id AND
	l.id = v.lang_id AND
	k.key IN ({0})
	AND l.language LIKE '%({1})';";

            string temp = string.Empty;
            foreach (string t in request.Keys)
            {
                temp += "'" + t + "',";
            }
            qry = string.Format(qry, temp.Substring(0, temp.Length - 1), request.Locale);
            EbDataTable datatbl = this.EbConnectionFactory.ObjectsDB.DoQuery(qry, new DbParameter[] { });

            foreach (EbDataRow dr in datatbl.Rows)
            {
                Dict.Add(dr["key"].ToString(), dr["value"].ToString());
            }

            return new GetDictionaryValueResponse { Dict = Dict };
        }

        //======================================= INSERT OR UPDATE RECORD =============================================

        public InsertDataFromWebformResponse Any(InsertDataFromWebformRequest request)
        {
            EbWebForm FormObj = GetWebFormObject(request.RefId);
            FormObj.TableRowId = request.RowId;
            if (FormObj.TableRowId > 0)
            {
                WebformData FormData = GetWebformData(request.RefId, request.RowId);
                InsertDataFromWebformResponse resp = UpdateDataFromWebformRec(request, FormObj);
                //if(resp.RowAffected > 0)
                //{
                //	//UpdateAuditTrail(OldData, request.Values, request.RefId, FormObj.TableRowId, request.UserId);/////////////////////////
                //}				
                return resp;
            }
            else
            {
                InsertDataFromWebformResponse resp = InsertDataFromWebformRec(request, FormObj);
                //if(resp.RowAffected > 0)
                //	UpdateAuditTrail(request.Values, request.RefId, resp.RowAffected, request.UserId);
                return resp;
            }
        }

        private InsertDataFromWebformResponse InsertDataFromWebformRec(InsertDataFromWebformRequest request, EbControlContainer FormObj)
        {
            string fullqry = string.Empty;
            List<DbParameter> param = new List<DbParameter>();
            int count = 0;
            foreach (KeyValuePair<string, SingleTable> entry in request.FormData.MultipleTables)
            {
                int i = 0;
                foreach (SingleRow row in entry.Value)
                {
                    string _qry = "INSERT INTO {0} ({1} eb_created_by, eb_created_at {3} ) VALUES ({2} :eb_createdby, :eb_createdat {4});";
                    string _tblname = entry.Key;
                    string _cols = string.Empty;
                    string _values = string.Empty;
                    //_cols = FormObj.GetCtrlNamesOfTable(entry.Key);

                    foreach (SingleColumn rField in row.Columns)
                    {
                        if (!rField.Name.Equals("id"))
                        {
                            _cols += string.Concat(rField.Name, ", ");
                            _values += string.Concat(":", rField.Name, "_", i, ", ");
                            param.Add(this.EbConnectionFactory.DataDB.GetNewParameter(rField.Name + "_" + i, (EbDbTypes)rField.Type, rField.Value));                 
                        }
                    }
                    i++;

                    if (count == 0)
                        _qry = _qry.Replace("{3}", "").Replace("{4}", "");
                    else
                        _qry = _qry.Replace("{3}", string.Concat(",", FormObj.TableName, "_id")).Replace("{4}", string.Concat(", (SELECT cur_val('", FormObj.TableName, "_id_seq'" + "))"));
                    fullqry += string.Format(_qry, _tblname, _cols, _values);
                }
                count++;

            }
            param.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_createdby", EbDbTypes.Int32, request.UserId));
            param.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_createdat", EbDbTypes.DateTime, System.DateTime.Now));
            fullqry += string.Concat("SELECT cur_val('", FormObj.TableName, "_id_seq');");

            var temp = EbConnectionFactory.DataDB.DoQuery(fullqry, param.ToArray());

            return new InsertDataFromWebformResponse { RowAffected = temp.Rows.Count > 0 ? Convert.ToInt32(temp.Rows[0][0]) : 0 };
        }

        private InsertDataFromWebformResponse UpdateDataFromWebformRec(InsertDataFromWebformRequest request, EbControlContainer FormObj)
        {
            string fullqry = string.Empty;
            List<DbParameter> param = new List<DbParameter>();
            foreach (KeyValuePair<string, SingleTable> entry in request.FormData.MultipleTables)
            {
				int i = 0;
				foreach (SingleRow row in entry.Value)
                {
                    string _tblname = entry.Key;
                    if (Convert.ToInt32(row.RowId) > 0)
                    {
                        string _qry = "UPDATE {0} SET {1} eb_lastmodified_by = :eb_modified_by, eb_lastmodified_at = :eb_modified_at WHERE id={2};";                        
                        string _colvals = string.Empty;

                        foreach (SingleColumn rField in row.Columns)
                        {
                            _colvals += string.Concat(rField.Name, "=:", rField.Name, "_", i, ",");
                            param.Add(this.EbConnectionFactory.DataDB.GetNewParameter(rField.Name + "_" + i, (EbDbTypes)rField.Type, rField.Value));
                        }
                        fullqry += string.Format(_qry, _tblname, _colvals, row.RowId);
                    }
                    else
                    {
                        string _qry = "INSERT INTO {0} ({1} eb_created_by, eb_created_at, {3}_id ) VALUES ({2} :eb_createdby, :eb_createdat ,:{4}_id);";
                        string _cols = string.Empty, _vals = string.Empty;
                        foreach (SingleColumn rField in row.Columns)
                        {
                            _cols += string.Concat(rField.Name, ",");
                            _vals += string.Concat(":", rField.Name, "_", i, ",");
                            param.Add(this.EbConnectionFactory.DataDB.GetNewParameter(rField.Name + "_" + i, (EbDbTypes)rField.Type, rField.Value));
                        }
                        fullqry += string.Format(_qry, _tblname, _cols, _vals, request.FormData.MasterTable, request.FormData.MasterTable);
                        param.Add(this.EbConnectionFactory.DataDB.GetNewParameter(request.FormData.MasterTable + "_id", EbDbTypes.Int32, request.FormData.MultipleTables[request.FormData.MasterTable][0].RowId));
                    }
                    i++;
                }
            }
            param.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_createdby", EbDbTypes.Int32, request.UserId));
            param.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_createdat", EbDbTypes.DateTime, System.DateTime.Now));
            param.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_modified_by", EbDbTypes.Int32, request.UserId));
            param.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_modified_at", EbDbTypes.DateTime, System.DateTime.Now));
            int rowsAffected = EbConnectionFactory.DataDB.InsertTable(fullqry, param.ToArray());

            return new InsertDataFromWebformResponse { RowAffected = rowsAffected };
        }


        // VALIDATION AND AUDIT TRAIL

        //private Dictionary<string, List<SingleColumn>> getFormDataAsColl(EbControlContainer FormObj)
        //{
        //    Dictionary<string, List<SingleColumn>> oldData = new Dictionary<string, List<SingleColumn>>();
        //    //FormObj.TableRowId = request.RowId;
        //    string query = FormObj.GetSelectQuery(FormObj.TableName);
        //    EbDataSet dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(query);

        //    foreach (EbDataTable dataTable in dataset.Tables)
        //    {
        //        List<SingleColumn> tblRecordColl = new List<SingleColumn>();
        //        foreach (EbDataRow dataRow in dataTable.Rows)
        //        {
        //            foreach (EbDataColumn dataColumn in dataTable.Columns)
        //            {
        //                object _unformattedData = dataRow[dataColumn.ColumnIndex];
        //                object _formattedData = _unformattedData;

        //                if (dataColumn.Type == EbDbTypes.Date)
        //                {
        //                    _unformattedData = (_unformattedData == DBNull.Value) ? DateTime.MinValue : _unformattedData;
        //                    _formattedData = ((DateTime)_unformattedData).Date != DateTime.MinValue ? Convert.ToDateTime(_unformattedData).ToString("yyyy-MM-dd") : string.Empty;
        //                }
        //                tblRecordColl.Add(new SingleColumn
        //                {
        //                    Name = dataColumn.ColumnName,
        //                    Type = (int)dataColumn.Type,
        //                    Value = _formattedData
        //                });
        //            }
        //        }
        //        oldData.Add(dataTable.TableName, tblRecordColl);
        //    }
        //    return oldData;
        //}

        private void UpdateAuditTrail(WebformData _NewData, string _FormId, int _RecordId, int _UserId)
        {
            List<AuditTrailEntry> FormFields = new List<AuditTrailEntry>();
            foreach (KeyValuePair<string, SingleTable> entry in _NewData.MultipleTables)
            {
                foreach (SingleRow rField in entry.Value)
                {
                    foreach(SingleColumn cField in rField.Columns)
                    {
                        FormFields.Add(new AuditTrailEntry
                        {
                            Name = cField.Name,
                            NewVal = cField.Value,
                            OldVal = string.Empty,
                            DataRel = _RecordId.ToString()
                        });
                    }                    
                }
            }
            if (FormFields.Count > 0)
                UpdateAuditTrail(FormFields, _FormId, _RecordId, _UserId);
        }

        private void UpdateAuditTrail(WebformData _OldData, WebformData _NewData, string _FormId, int _RecordId, int _UserId)
        {
            List<AuditTrailEntry> FormFields = new List<AuditTrailEntry>();
            foreach (KeyValuePair<string, SingleTable> entry in _OldData.MultipleTables)
            {
                if (_NewData.MultipleTables.ContainsKey(entry.Key))
                {
                    foreach (SingleRow rField in entry.Value)
                    {
                        SingleRow nrF = _NewData.MultipleTables[entry.Key].Find(e => e.RowId == rField.RowId);
                        foreach(SingleColumn cField in rField.Columns)
                        {
                            SingleColumn ncf = nrF.Columns.Find(e => e.Name == cField.Name);

                            if (ncf != null && ncf.Value != cField.Value)
                            {
                                FormFields.Add(new AuditTrailEntry
                                {
                                    Name = cField.Name,
                                    NewVal = ncf.Value,
                                    OldVal = cField.Value,
                                    DataRel = string.Concat(_RecordId, "-", rField.RowId)
                                });
                            }
                        }                        
                    }
                }
            }
            if (FormFields.Count > 0)
                UpdateAuditTrail(FormFields, _FormId, _RecordId, _UserId);
        }

        private void UpdateAuditTrail(List<AuditTrailEntry> _Fields, string _FormId, int _RecordId, int _UserId)
        {
            List<DbParameter> parameters = new List<DbParameter>();
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("formid", EbDbTypes.String, _FormId));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("dataid", EbDbTypes.Int32, _RecordId));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_createdby", EbDbTypes.Int32, _UserId));
            string Qry = "INSERT INTO eb_audit_master(formid, dataid, eb_createdby, eb_createdat) VALUES (:formid, :dataid, :eb_createdby, NOW()) RETURNING id;";
            EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery(Qry, parameters.ToArray());
            var id = Convert.ToInt32(dt.Rows[0][0]);

            string lineQry = "INSERT INTO eb_audit_lines(masterid, fieldname, oldvalue, newvalue, idrelation) VALUES ";
            List<DbParameter> parameters1 = new List<DbParameter>();
            parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter("masterid", EbDbTypes.Int32, id));
            for (int i = 0; i < _Fields.Count; i++)
            {
                lineQry += string.Format("(:masterid, :{0}_{1}, :old{0}_{1}, :new{0}_{1}, :idrel{0}_{1}),", _Fields[i].Name, i);
                parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter(_Fields[i].Name + "_" + i, EbDbTypes.String, _Fields[i].Name));
                parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter("new" + _Fields[i].Name + "_" + i, EbDbTypes.String, _Fields[i].NewVal));
                parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter("old" + _Fields[i].Name + "_" + i, EbDbTypes.String, _Fields[i].OldVal));
                parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter("idrel" + _Fields[i].Name + "_" + i, EbDbTypes.String, _Fields[i].DataRel));
            }
            var rrr = this.EbConnectionFactory.ObjectsDB.DoNonQuery(lineQry.Substring(0, lineQry.Length - 1), parameters1.ToArray());
        }

        public GetAuditTrailResponse Any(GetAuditTrailRequest request)
        {
            string qry = @"	SELECT 
								m.id, u.fullname, m.eb_createdat, l.fieldname, l.oldvalue, l.newvalue
							FROM 
								eb_audit_master m, eb_audit_lines l, eb_users u
							WHERE
								m.id = l.masterid AND m.eb_createdby = u.id AND m.formid = :formid AND m.dataid = :dataid
							ORDER BY
								m.id , l.fieldname;";
            DbParameter[] parameters = new DbParameter[] {
                this.EbConnectionFactory.DataDB.GetNewParameter("formid", EbDbTypes.String, request.FormId),
                this.EbConnectionFactory.DataDB.GetNewParameter("dataid", EbDbTypes.Int32, request.RowId)
            };
            EbDataTable dt = EbConnectionFactory.DataDB.DoQuery(qry, parameters);

            Dictionary<int, FormTransaction> logs = new Dictionary<int, FormTransaction>();

            foreach (EbDataRow dr in dt.Rows)
            {
                int id = 1048576 - Convert.ToInt32(dr["id"]);
                if (logs.ContainsKey(id))
                {
                    logs[id].Details.Add(new FormTransactionLine
                    {
                        FieldName = dr["fieldname"].ToString(),
                        OldValue = dr["oldvalue"].ToString(),
                        NewValue = dr["newvalue"].ToString()
                    });
                }
                else
                {
                    logs.Add(id, new FormTransaction
                    {
                        CreatedBy = dr["fullname"].ToString(),
                        CreatedAt = Convert.ToDateTime(dr["eb_createdat"]).ToString("dd-MM-yyyy hh:mm:ss tt"),
                        Details = new List<FormTransactionLine>() {
                            new FormTransactionLine {
                                FieldName = dr["fieldname"].ToString(),
                                OldValue = dr["oldvalue"].ToString(),
                                NewValue = dr["newvalue"].ToString()
                            }
                        }
                    });
                }
            }

            return new GetAuditTrailResponse { Logs = logs };
        }

        //=============================================== MISCELLANEOUS ====================================================

        public GetDesignHtmlResponse Post(GetDesignHtmlRequest request)
        {
            var myService = base.ResolveService<EbObjectService>();
            EbObjectParticularVersionResponse formObj = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });

            EbUserControl _uc = EbSerializers.Json_Deserialize(formObj.Data[0].Json);
            _uc.AfterRedisGet(this);
            _uc.VersionNumber = formObj.Data[0].VersionNumber;//Version number(w) in EbObject is not updated when it is commited
            string _temp = _uc.GetInnerHtml();

            return new GetDesignHtmlResponse {Html = _temp };
        }
    }
}
