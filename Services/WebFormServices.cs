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
            CreateWebFormTableRec(request.WebObj, request.WebObj.TableName);
            return new CreateWebFormTableResponse { };
        }

        private void CreateWebFormTableRec(EbControlContainer _container, string _table)
        {
            CreateWebFormTableHelper(_container, _table);
            foreach (EbControl _control in _container.Controls)
            {
                if (_control is EbControlContainer)
                {
                    EbControlContainer Container = _control as EbControlContainer;

                    if (Container.TableName.IsNullOrEmpty())
                    {
                        Container.TableName = _container.TableName;
                    }
                    CreateWebFormTableRec(Container, _container.TableName);
                }
            }
        }

        private void CreateWebFormTableHelper(EbControlContainer _container, string _table)
        {
            IVendorDbTypes vDbTypes = this.EbConnectionFactory.ObjectsDB.VendorDbTypes;
            List<TableColumnMeta> _listNamesAndTypes = new List<TableColumnMeta>();
            IEnumerable<EbControl> _flatControls = _container.Controls.Get1stLvlControls();

            foreach (EbControl control in _flatControls)
            {
                _listNamesAndTypes.Add(new TableColumnMeta { Name = control.Name, Type = control.GetvDbType(vDbTypes) });
            }
            if (_listNamesAndTypes.Count > 0)
            {
                if (!_table.ToLower().Equals(_container.TableName.ToLower()))
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = _table + "_id", Type = vDbTypes.Decimal });// id refernce to the parent table will store in this column - foreignkey
                _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_by", Type = vDbTypes.Decimal });
                _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_at", Type = vDbTypes.DateTime });
                _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_by", Type = vDbTypes.Decimal });
                _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_at", Type = vDbTypes.DateTime });
                _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_del", Type = vDbTypes.Boolean, Default = "F" });
                _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_void", Type = vDbTypes.Boolean, Default = "F" });
                _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_transaction_date", Type = vDbTypes.DateTime });
                _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_autogen", Type = vDbTypes.Decimal });

                CreateOrAlterTable(_container.TableName.ToLower(), _listNamesAndTypes);
            }
        }

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
            EbWebForm FormObj = GetWebFormObject(request.RefId);
            FormObj.TableRowId = request.RowId;
            string query = FormObj.GetSelectQuery(FormObj.TableName);
            EbDataSet dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(query);

            GetRowDataResponse _dataset = new GetRowDataResponse();
            _dataset.FormData = getDataSetAsRowCollection(dataset);
            return _dataset;
        }

        private WebformData getDataSetAsRowCollection(EbDataSet dataset)
        {
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
                        Row.Columns.Add(new SingleColumn() {
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
            var myService = base.ResolveService<EbObjectService>();
            EbObjectParticularVersionResponse formObj = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = RefId });
            return EbSerializers.Json_Deserialize(formObj.Data[0].Json);
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
                Dictionary<string, List<SingleColumn>> OldData = getFormDataAsColl(FormObj);
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
                    string _qry = "INSERT INTO {0} ({1}, eb_created_by, eb_created_at {3} ) VALUES ({2} :eb_createdby, :eb_createdat {4});";
                    string _tblname = entry.Key;
                    string _cols = string.Empty;
                    string _values = string.Empty;
                    _cols = FormObj.GetCtrlNamesOfTable(entry.Key);

                    foreach (SingleColumn rField in row.Columns)
                    {
                        if (!rField.Name.Equals("id"))
                        {
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
                    string _qry = "UPDATE {0} SET {1} eb_lastmodified_by = :eb_modified_by, eb_lastmodified_at = :eb_modified_at WHERE id={2};";
                    string _tblname = entry.Key;
                    string _colvals = string.Empty;

                    foreach (SingleColumn rField in row.Columns)
                    {
                        _colvals += string.Concat(rField.Name, "=:", rField.Name, "_", i, ",");
                        param.Add(this.EbConnectionFactory.DataDB.GetNewParameter(rField.Name + "_" + i, (EbDbTypes)rField.Type, rField.Value));
                    }
					i++;
                    fullqry += string.Format(_qry, _tblname, _colvals, row.RowId);                    
                }
            }
            param.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_modified_by", EbDbTypes.Int32, request.UserId));
            param.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_modified_at", EbDbTypes.DateTime, System.DateTime.Now));
            int rowsAffected = EbConnectionFactory.DataDB.InsertTable(fullqry, param.ToArray());

            return new InsertDataFromWebformResponse { RowAffected = rowsAffected };
        }


        // VALIDATION AND AUDIT TRAIL

        private Dictionary<string, List<SingleColumn>> getFormDataAsColl(EbControlContainer FormObj)
        {
            Dictionary<string, List<SingleColumn>> oldData = new Dictionary<string, List<SingleColumn>>();
            //FormObj.TableRowId = request.RowId;
            string query = FormObj.GetSelectQuery(FormObj.TableName);
            EbDataSet dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(query);

            foreach (EbDataTable dataTable in dataset.Tables)
            {
                List<SingleColumn> tblRecordColl = new List<SingleColumn>();
                foreach (EbDataRow dataRow in dataTable.Rows)
                {
                    foreach (EbDataColumn dataColumn in dataTable.Columns)
                    {
                        object _unformattedData = dataRow[dataColumn.ColumnIndex];
                        object _formattedData = _unformattedData;

                        if (dataColumn.Type == EbDbTypes.Date)
                        {
                            _unformattedData = (_unformattedData == DBNull.Value) ? DateTime.MinValue : _unformattedData;
                            _formattedData = ((DateTime)_unformattedData).Date != DateTime.MinValue ? Convert.ToDateTime(_unformattedData).ToString("yyyy-MM-dd") : string.Empty;
                        }
                        tblRecordColl.Add(new SingleColumn
                        {
                            Name = dataColumn.ColumnName,
                            Type = (int)dataColumn.Type,
                            Value = _formattedData
                        });
                    }
                }
                oldData.Add(dataTable.TableName, tblRecordColl);
            }
            return oldData;
        }

        private void UpdateAuditTrail(Dictionary<string, List<SingleColumn>> _NewData, string _FormId, int _RecordId, int _UserId)
        {
            List<SingleColumn> FormFields = new List<SingleColumn>();
            foreach (KeyValuePair<string, List<SingleColumn>> entry in _NewData)
            {
                foreach (SingleColumn rField in entry.Value)
                {
                    FormFields.Add(new SingleColumn
                    {
                        Name = rField.Name,
                        Type = rField.Type,
                        Value = rField.Value,
                        OldValue = string.Empty
                    });
                }
            }
            if (FormFields.Count > 0)
                UpdateAuditTrail(FormFields, _FormId, _RecordId, _UserId);
        }

        private void UpdateAuditTrail(Dictionary<string, List<SingleColumn>> _OldData, Dictionary<string, List<SingleColumn>> _NewData, string _FormId, int _RecordId, int _UserId)
        {
            List<SingleColumn> FormFields = new List<SingleColumn>();
            foreach (KeyValuePair<string, List<SingleColumn>> entry in _OldData)
            {
                if (_NewData.ContainsKey(entry.Key))
                {
                    foreach (SingleColumn rField in entry.Value)
                    {
                        SingleColumn nrF = _NewData[entry.Key].Find(e => e.Name == rField.Name);
                        if (nrF != null && nrF.Value != rField.Value)
                        {
                            FormFields.Add(new SingleColumn
                            {
                                Name = rField.Name,
                                Type = rField.Type,
                                Value = nrF.Value,
                                OldValue = rField.Value
                            });
                        }
                    }
                }
            }
            if (FormFields.Count > 0)
                UpdateAuditTrail(FormFields, _FormId, _RecordId, _UserId);
        }

        private void UpdateAuditTrail(List<SingleColumn> _Fields, string _FormId, int _RecordId, int _UserId)
        {
            List<DbParameter> parameters = new List<DbParameter>();
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("formid", EbDbTypes.String, _FormId));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("dataid", EbDbTypes.Int32, _RecordId));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_createdby", EbDbTypes.Int32, _UserId));
            string Qry = "INSERT INTO eb_audit_master(formid, dataid, eb_createdby, eb_createdat) VALUES (:formid, :dataid, :eb_createdby, NOW()) RETURNING id;";
            EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery(Qry, parameters.ToArray());
            var id = Convert.ToInt32(dt.Rows[0][0]);

            string lineQry = "INSERT INTO eb_audit_lines(masterid, fieldname, oldvalue, newvalue) VALUES ";
            List<DbParameter> parameters1 = new List<DbParameter>();
            parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter("masterid", EbDbTypes.Int32, id));
            for (int i = 0; i < _Fields.Count; i++)
            {
                lineQry += "(:masterid, :" + _Fields[i].Name + ", :old" + _Fields[i].Name + ", :new" + _Fields[i].Name + "),";
                parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter(_Fields[i].Name, EbDbTypes.String, _Fields[i].Name));
                parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter("new" + _Fields[i].Name, EbDbTypes.String, _Fields[i].Value));
                parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter("old" + _Fields[i].Name, EbDbTypes.String, _Fields[i].OldValue));
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
    }
}
