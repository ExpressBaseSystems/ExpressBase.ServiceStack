using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Jurassic;
using Jurassic.Library;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using Newtonsoft.Json;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Dynamic;
using System.Globalization;
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
            {
                (request.WebObj as EbWebForm).AfterRedisGet(this);
                CreateWebFormTables((request.WebObj as EbWebForm).FormSchema);
            }

            //CreateWebFormTableRec(request.WebObj, request.WebObj.TableName);

            //WebFormSchema _temp = GetWebFormSchema(request.WebObj);/////////
            //CreateWebFormTables(_temp);

            return new CreateWebFormTableResponse { };
        }

        private void CreateWebFormTables(WebFormSchema _schema)
        {
            IVendorDbTypes vDbTypes = this.EbConnectionFactory.ObjectsDB.VendorDbTypes;
            string Msg = string.Empty;
            foreach (TableSchema _table in _schema.Tables)
            {
                List<TableColumnMeta> _listNamesAndTypes = new List<TableColumnMeta>();
                if (_table.Columns.Count > 0)
                {
                    foreach (ColumnSchema _column in _table.Columns)
                    {
                        if (_column.Control is EbAutoId)
                        {
                            _listNamesAndTypes.Add(new TableColumnMeta { Name = _column.ColumnName, Type = vDbTypes.GetVendorDbTypeStruct((EbDbTypes)_column.EbDbType), Unique = true });
                            _listNamesAndTypes.Add(new TableColumnMeta { Name = _column.ColumnName + "_ebbkup", Type = vDbTypes.GetVendorDbTypeStruct((EbDbTypes)_column.EbDbType) });
                        }
                        else
                            _listNamesAndTypes.Add(new TableColumnMeta { Name = _column.ColumnName, Type = vDbTypes.GetVendorDbTypeStruct((EbDbTypes)_column.EbDbType) });
                    }
                    if (_table.TableName != _schema.MasterTable)
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = _schema.MasterTable + "_id", Type = vDbTypes.Decimal });// id refernce to the parent table will store in this column - foreignkey

                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_by", Type = vDbTypes.Decimal });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_at", Type = vDbTypes.DateTime });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_by", Type = vDbTypes.Decimal });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_at", Type = vDbTypes.DateTime });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_del", Type = vDbTypes.Boolean, Default = "F" });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_void", Type = vDbTypes.Boolean, Default = "F" });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_loc_id", Type = vDbTypes.Int32 });
                    //_listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_default", Type = vDbTypes.Boolean, Default = "F" });
                    //_listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_transaction_date", Type = vDbTypes.DateTime });
                    //_listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_autogen", Type = vDbTypes.Decimal });

                    CreateOrAlterTable(_table.TableName, _listNamesAndTypes, ref Msg);
                }
            }
            if (!Msg.IsEmpty())
                throw new FormException(Msg);
        }

        private int CreateOrAlterTable(string tableName, List<TableColumnMeta> listNamesAndTypes, ref string Msg)
        {
            //checking for space in column name, table name
            if (tableName.Contains(CharConstants.SPACE))
                throw new FormException("Table creation failed - Invalid table name: " + tableName);
            foreach (TableColumnMeta entry in listNamesAndTypes)
                if (entry.Name.Contains(CharConstants.SPACE))
                    throw new FormException("Table creation failed : Invalid column name" + entry.Name);

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
                else if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.PGSQL)
                {
                    sql = "CREATE TABLE @tbl( id SERIAL PRIMARY KEY, @cols)".Replace("@cols", cols).Replace("@tbl", tableName);
                    this.EbConnectionFactory.ObjectsDB.CreateTable(sql);
                }
                else if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.MYSQL)
                {
                    sql = "CREATE TABLE @tbl( id INTEGER AUTO_INCREMENT PRIMARY KEY, @cols)".Replace("@cols", cols).Replace("@tbl", tableName);
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
                            if (entry.Type.EbDbType != dr.Type && !(entry.Name.Equals("eb_created_at") ||
                                entry.Name.Equals("eb_lastmodified_at") || entry.Name.Equals("eb_del") ||
                                entry.Name.Equals("eb_void") || entry.Name.Equals("eb_default") ||
                                (entry.Type.EbDbType.ToString().Equals("Boolean") && dr.Type.ToString().Equals("String"))))
                                Msg += string.Format("Already exists '{0}' Column for {1}.{2}({3}); ", dr.Type.ToString(), tableName, entry.Name, entry.Type.EbDbType);
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
                    else if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.PGSQL)
                    {
                        sql = (appendId ? "id SERIAL PRIMARY KEY," : "") + sql;
                        if (!sql.IsEmpty())
                        {
                            sql = "ALTER TABLE @tbl ADD COLUMN " + (sql.Substring(0, sql.Length - 1)).Replace(",", ", ADD COLUMN ");
                            sql = sql.Replace("@tbl", tableName);
                            this.EbConnectionFactory.ObjectsDB.UpdateTable(sql);
                        }
                    }
                    else if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.MYSQL)
                    {
                        sql = (appendId ? "id INTEGER AUTO_INCREMENT PRIMARY KEY," : "") + sql;
                        if (!sql.IsEmpty())
                        {
                            sql = "ALTER TABLE @tbl ADD COLUMN " + (sql.Substring(0, sql.Length - 1)).Replace(",", ", ADD COLUMN ");
                            sql = sql.Replace("@tbl", tableName);
                            this.EbConnectionFactory.ObjectsDB.UpdateTable(sql);
                        }
                    }
                    return 0;
                }
            }
            return -1;
            //throw new FormException("Table creation failed - Table name: " + tableName);
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
            Console.WriteLine("Requesting for WebFormData( Refid : " + request.RefId + ", Rowid : " + request.RowId + " ).................");
            GetRowDataResponse _dataset = new GetRowDataResponse();
            EbWebForm form = GetWebFormObject(request.RefId);
            form.TableRowId = request.RowId;
            form.RefId = request.RefId;
            form.UserObj = request.UserObj;
            form.RefreshFormData(EbConnectionFactory.DataDB, this);
            _dataset.FormData = form.FormData;
            return _dataset;
        }

        public GetPrefillDataResponse Any(GetPrefillDataRequest request)
        {
            GetPrefillDataResponse _dataset = new GetPrefillDataResponse();
            EbWebForm form = GetWebFormObject(request.RefId);
            form.RefId = request.RefId;
            form.RefreshFormData(EbConnectionFactory.DataDB, this, request.Params);
            _dataset.FormData = form.FormData;
            return _dataset;
        }

        private EbWebForm GetWebFormObject(string RefId)
        {
            EbWebForm _form = this.Redis.Get<EbWebForm>(RefId);
            if (_form == null)
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
            string qry = @"SELECT k.key, v.value 
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

        //======================================= INSERT OR UPDATE OR DELETE RECORD =============================================

        public InsertDataFromWebformResponse Any(InsertDataFromWebformRequest request)
        {
            EbWebForm FormObj = GetWebFormObject(request.RefId);
            FormObj.RefId = request.RefId;
            FormObj.TableRowId = request.RowId;
            FormObj.FormData = request.FormData;
            FormObj.UserObj = request.UserObj;
            FormObj.LocationId = request.CurrentLoc;

            Console.WriteLine("Insert/Update WebFormData : MergeFormData start");
            FormObj.MergeFormData();
            Console.WriteLine("Insert/Update WebFormData : Save start");

            int r = FormObj.Save(EbConnectionFactory.DataDB, this);

            Console.WriteLine("Insert/Update WebFormData : AfterSave start");
            int a = FormObj.AfterSave(EbConnectionFactory.DataDB, request.RowId > 0);
            return new InsertDataFromWebformResponse()
            {
                RowId = FormObj.TableRowId,
                FormData = FormObj.FormData,
                RowAffected = r,
                AfterSaveStatus = a
            };
        }

        public DeleteDataFromWebformResponse Any(DeleteDataFromWebformRequest request)
        {
            EbWebForm FormObj = GetWebFormObject(request.RefId);
            FormObj.TableRowId = request.RowId;
            FormObj.UserObj = request.UserObj;
            return new DeleteDataFromWebformResponse
            {
                RowAffected = FormObj.Delete(EbConnectionFactory.DataDB)
            };
        }

        public CancelDataFromWebformResponse Any(CancelDataFromWebformRequest request)
        {
            EbWebForm FormObj = GetWebFormObject(request.RefId);
            FormObj.TableRowId = request.RowId;
            FormObj.UserObj = request.UserObj;
            return new CancelDataFromWebformResponse
            {
                RowAffected = FormObj.Cancel(EbConnectionFactory.DataDB)
            };
        }


        //================================= FORMULA AND VALIDATION =================================================

        public WebformData CalcFormula(WebformData _formData, EbWebForm _formObj)
        {
            Dictionary<int, EbControlWrapper> ctrls = EbWebForm.GetControlsAsDict(_formObj, "FORM");
            List<int> ExeOrder = GetExecutionOrder(ctrls);

            for (int i = 0; i < ExeOrder.Count; i++)
            {
                EbControlWrapper cw = ctrls[ExeOrder[i]];
                Script valscript = CSharpScript.Create<dynamic>(
                    cw.Control.ValueExpr.Code,
                    ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core").WithImports("System.Dynamic", "System", "System.Collections.Generic",
                    "System.Diagnostics", "System.Linq"),
                    globalsType: typeof(FormGlobals)
                );
                valscript.Compile();

                FormAsGlobal g = _formObj.GetFormAsGlobal(_formData);
                FormGlobals globals = new FormGlobals() { FORM = g };
                var result = (valscript.RunAsync(globals)).Result.ReturnValue;

                _formData.MultipleTables[cw.TableName][0].Columns.Add(new SingleColumn
                {
                    Name = cw.Control.Name,
                    Type = (int)cw.Control.EbDbType,
                    Value = result
                });
            }
            return _formData;
        }

        private List<int> GetExecutionOrder(Dictionary<int, EbControlWrapper> ctrls)
        {
            List<int> CalcFlds = new List<int>() { 1, 2, 4 };//
            List<int> ExeOrd = new List<int>();
            List<KeyValuePair<int, int>> dpndcy = new List<KeyValuePair<int, int>>();
            for (int i = 0; i < CalcFlds.Count; i++)
            {
                if (ctrls[CalcFlds[i]].Control.ValueExpr.Code.Contains("FORM"))//testing purpose
                {
                    for (int j = 0; j < CalcFlds.Count; j++)
                    {
                        if (i != j)
                        {
                            if (ctrls[CalcFlds[i]].Control.ValueExpr.Code.Contains(ctrls[CalcFlds[i]].Path))
                                dpndcy.Add(new KeyValuePair<int, int>(i, j));
                        }
                    }
                }
            }
            int count = 0;
            while (dpndcy.Count > 0 && count < CalcFlds.Count)
            {
                for (int i = 0; i < CalcFlds.Count; i++)
                {
                    var t = dpndcy.FindIndex(x => x.Key == CalcFlds[i]);
                    if (t == -1)
                    {
                        ExeOrd.Add(CalcFlds[i]);
                        dpndcy.RemoveAll(x => x.Value == CalcFlds[i]);
                    }
                }
                count++;
            }

            return ExeOrd;
        }

        //incomplete
        public bool ValidateFormData(WebformData _formData, EbWebForm _formObj)
        {
            var engine = new ScriptEngine();
            ObjectInstance globals = GetJsFormGlobal(_formData, _formObj, engine);
            engine.SetGlobalValue("FORM", globals);
            List<EbValidator> warnings = new List<EbValidator>();
            List<EbValidator> errors = new List<EbValidator>();
            Dictionary<int, EbControlWrapper> ctrls = EbWebForm.GetControlsAsDict(_formObj, "FORM");
            foreach (KeyValuePair<int, EbControlWrapper> ctrl in ctrls)
            {
                for (int i = 0; i < ctrl.Value.Control.Validators.Count; i++)
                {
                    EbValidator v = ctrl.Value.Control.Validators[i];
                    if (!v.IsDisabled)
                    {
                        string fn = v.Name + ctrl.Key;
                        engine.Evaluate("function " + fn + "() { " + v.Script.Code + " }");
                        if (!engine.CallGlobalFunction<bool>(fn))
                        {
                            if (v.IsWarningOnly)
                                warnings.Add(v);
                            else
                                errors.Add(v);
                        }
                    }
                }
            }
            return (errors.Count > 0);
        }


        //get formdata as globals for Jurassic script engine
        private ObjectInstance GetJsFormGlobal(WebformData _formData, EbControlContainer _container, ScriptEngine _engine, ObjectInstance _globals = null)
        {
            if (_globals == null)
            {
                _globals = _engine.Object.Construct();
            }
            if (_formData.MultipleTables.ContainsKey(_container.TableName))
            {
                if (_formData.MultipleTables[_container.TableName].Count > 0)
                {
                    foreach (EbControl control in _container.Controls)
                    {
                        if (_container is EbDataGrid)
                        {
                            EbDataGrid dg = _container as EbDataGrid;
                            ArrayInstance a = _engine.Array.Construct(_formData.MultipleTables[_container.TableName].Count);
                            _globals[control.Name] = a;
                            for (int i = 0; i < _formData.MultipleTables[_container.TableName].Count; i++)
                            {
                                ObjectInstance g = _engine.Object.Construct();
                                a[i] = g;
                                foreach (EbControl c in dg.Controls)
                                {
                                    g[c.Name] = GetDataByControlName(_formData, dg.TableName, c.Name, i);
                                }
                            }
                        }
                        else if (control is EbControlContainer)
                        {
                            ObjectInstance g = _engine.Object.Construct();
                            _globals[control.Name] = g;
                            g = GetJsFormGlobal(_formData, control as EbControlContainer, _engine, g);
                        }
                        else
                        {
                            _globals[control.Name] = GetDataByControlName(_formData, _container.TableName, control.Name, 0);
                        }
                    }
                }
            }
            return _globals;
        }

        private dynamic GetDataByControlName(WebformData _formData, string _table, string _column, int _row = 0)
        {
            try
            {
                return _formData.MultipleTables[_table][_row][_column];
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception!!! : " + e.Message);
                return null;
            }
        }


        private void JurassicTest()
        {
            //var engine = new ScriptEngine();
            ////engine.Execute("console.log('testing')");
            ////engine.SetGlobalValue("interop", 15);

            //var segment = engine.Object.Construct();
            //segment["type"] = "Feature";
            //segment["properties"] = engine.Object.Construct();
            //var geometry = engine.Object.Construct();
            //geometry["type"] = "LineString";
            //geometry["coordinates"] = engine.Array.Construct(
            //  engine.Array.Construct(-37.3, 121.5),
            //  engine.Array.Construct(-38.1, 122.6)
            //);
            //segment["geometry"] = geometry;

            //engine.SetGlobalValue("form", segment);
            //engine.SetGlobalValue("console", new Jurassic.Library.FirebugConsole(engine));

            //engine.Execute("console.log(form.properties.type)");

            var engine2 = new ScriptEngine();
            engine2.Evaluate("function test(a, b) { if(a%2 === 0) return true;else return false; }");
            Console.WriteLine(engine2.CallGlobalFunction<bool>("test", 5, 6));
        }

        private void CSTest()
        {
            //FormGlobals temp = new FormGlobals();

            //ListNTV t1 = new ListNTV() {
            //    Columns = new List<NTV>() {
            //    new NTV { Name = "demo11", Type = EbDbTypes.String, Value = "febin11" }
            //} };
            //ListNTV t2 = new ListNTV() {
            //    Columns = new List<NTV>() {
            //    new NTV { Name = "demo22", Type = EbDbTypes.String, Value = "febin22" },
            //    new NTV { Name = "demo33", Type = EbDbTypes.String, Value = "febin33" }
            //} };
            //ListNTV t3 = new ListNTV()
            //{
            //    Columns = new List<NTV>() {
            //    new NTV { Name = "demo44", Type = EbDbTypes.String, Value = "febin44" },
            //    new NTV { Name = "demo55", Type = EbDbTypes.String, Value = "febin55" }
            //}
            //};

            //temp.FORM.Rows.Add(t1);
            //var xxx = new FormAsGlobal
            //{
            //    Name = "demo66",
            //    Rows = new List<ListNTV> { t3 },
            //    Containers = new List<FormAsGlobal>()
            //};
            //temp.FORM.Containers.Add(new FormAsGlobal
            //{
            //    Name = "demo33",
            //    Rows = new List<ListNTV> { t2},
            //    Containers = new List<FormAsGlobal>() { xxx}
            //});

            //var xxxx = temp.FORM.demo33.demo22;

            string CsCode = "return FORM.demo33.demo22;";
            Script valscript = CSharpScript.Create<dynamic>(
                CsCode,
                ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core").WithImports("System.Dynamic", "System", "System.Collections.Generic",
                "System.Diagnostics", "System.Linq"),
                globalsType: typeof(FormGlobals));

            valscript.Compile();
            FormGlobals globals = new FormGlobals();
            var result = (valscript.RunAsync(globals)).Result.ReturnValue;

        }




        //===================================== AUDIT TRAIL ========================================================

        //private void UpdateAuditTrail(WebformData _NewData, string _FormId, int _RecordId, int _UserId)
        //{
        //    List<AuditTrailEntry> FormFields = new List<AuditTrailEntry>();
        //    foreach (KeyValuePair<string, SingleTable> entry in _NewData.MultipleTables)
        //    {
        //        foreach (SingleRow rField in entry.Value)
        //        {
        //            foreach (SingleColumn cField in rField.Columns)
        //            {
        //                FormFields.Add(new AuditTrailEntry
        //                {
        //                    Name = cField.Name,
        //                    NewVal = cField.Value,
        //                    OldVal = string.Empty,
        //                    DataRel = _RecordId.ToString()
        //                });
        //            }
        //        }
        //    }
        //    if (FormFields.Count > 0)
        //        UpdateAuditTrail(FormFields, _FormId, _RecordId, _UserId);
        //}

        //private void UpdateAuditTrail(WebformData _OldData, WebformData _NewData, string _FormId, int _RecordId, int _UserId)
        //{
        //    List<AuditTrailEntry> FormFields = new List<AuditTrailEntry>();
        //    foreach (KeyValuePair<string, SingleTable> entry in _OldData.MultipleTables)
        //    {
        //        if (_NewData.MultipleTables.ContainsKey(entry.Key))
        //        {
        //            foreach (SingleRow rField in entry.Value)
        //            {
        //                SingleRow nrF = _NewData.MultipleTables[entry.Key].Find(e => e.RowId == rField.RowId);
        //                foreach (SingleColumn cField in rField.Columns)
        //                {
        //                    SingleColumn ncf = nrF.Columns.Find(e => e.Name == cField.Name);

        //                    if (ncf != null && ncf.Value != cField.Value)
        //                    {
        //                        FormFields.Add(new AuditTrailEntry
        //                        {
        //                            Name = cField.Name,
        //                            NewVal = ncf.Value,
        //                            OldVal = cField.Value,
        //                            DataRel = string.Concat(_RecordId, "-", rField.RowId)
        //                        });
        //                    }
        //                }
        //            }
        //        }
        //    }
        //    if (FormFields.Count > 0)
        //        UpdateAuditTrail(FormFields, _FormId, _RecordId, _UserId);
        //}

        //private void UpdateAuditTrail(List<AuditTrailEntry> _Fields, string _FormId, int _RecordId, int _UserId)
        //{
        //    List<DbParameter> parameters = new List<DbParameter>();
        //    parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("formid", EbDbTypes.String, _FormId));
        //    parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("dataid", EbDbTypes.Int32, _RecordId));
        //    parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_createdby", EbDbTypes.Int32, _UserId));
        //    string Qry = "INSERT INTO eb_audit_master(formid, dataid, eb_createdby, eb_createdat) VALUES (:formid, :dataid, :eb_createdby, CURRENT_TIMESTAMP AT TIME ZONE 'UTC') RETURNING id;";
        //    EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery(Qry, parameters.ToArray());
        //    var id = Convert.ToInt32(dt.Rows[0][0]);

        //    string lineQry = "INSERT INTO eb_audit_lines(masterid, fieldname, oldvalue, newvalue, idrelation) VALUES ";
        //    List<DbParameter> parameters1 = new List<DbParameter>();
        //    parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter("masterid", EbDbTypes.Int32, id));
        //    for (int i = 0; i < _Fields.Count; i++)
        //    {
        //        lineQry += string.Format("(:masterid, :{0}_{1}, :old{0}_{1}, :new{0}_{1}, :idrel{0}_{1}),", _Fields[i].Name, i);
        //        parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter(_Fields[i].Name + "_" + i, EbDbTypes.String, _Fields[i].Name));
        //        parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter("new" + _Fields[i].Name + "_" + i, EbDbTypes.String, _Fields[i].NewVal));
        //        parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter("old" + _Fields[i].Name + "_" + i, EbDbTypes.String, _Fields[i].OldVal));
        //        parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter("idrel" + _Fields[i].Name + "_" + i, EbDbTypes.String, _Fields[i].DataRel));
        //    }
        //    var rrr = this.EbConnectionFactory.ObjectsDB.DoNonQuery(lineQry.Substring(0, lineQry.Length - 1), parameters1.ToArray());
        //}

        public GetAuditTrailResponse Any(GetAuditTrailRequest request)
        {
            EbWebForm FormObj = GetWebFormObject(request.FormId);
            FormObj.RefId = request.FormId;
            FormObj.TableRowId = request.RowId;
            FormObj.UserObj = request.UserObj;

            string temp = FormObj.GetAuditTrail(EbConnectionFactory.DataDB, this);

            return new GetAuditTrailResponse() { Json = temp };


            //     string qry = @"	SELECT 
            //	m.id, u.fullname, m.eb_createdat, l.fieldname, l.oldvalue, l.newvalue
            //FROM 
            //	eb_audit_master m, eb_audit_lines l, eb_users u
            //WHERE
            //	m.id = l.masterid AND m.eb_createdby = u.id AND m.formid = :formid AND m.dataid = :dataid
            //ORDER BY
            //	m.id , l.fieldname;";
            //     DbParameter[] parameters = new DbParameter[] {
            //         this.EbConnectionFactory.DataDB.GetNewParameter("formid", EbDbTypes.String, request.FormId),
            //         this.EbConnectionFactory.DataDB.GetNewParameter("dataid", EbDbTypes.Int32, request.RowId)
            //     };
            //     EbDataTable dt = EbConnectionFactory.DataDB.DoQuery(qry, parameters);

            //     Dictionary<int, FormTransaction> logs = new Dictionary<int, FormTransaction>();

            //     foreach (EbDataRow dr in dt.Rows)
            //     {
            //         int id = 1048576 - Convert.ToInt32(dr["id"]);
            //         if (logs.ContainsKey(id))
            //         {
            //             logs[id].Details.Add(new FormTransactionLine
            //             {
            //                 FieldName = dr["fieldname"].ToString(),
            //                 OldValue = dr["oldvalue"].ToString(),
            //                 NewValue = dr["newvalue"].ToString()
            //             });
            //         }
            //         else
            //         {
            //             logs.Add(id, new FormTransaction
            //             {
            //                 CreatedBy = dr["fullname"].ToString(),
            //                 CreatedAt = Convert.ToDateTime(dr["eb_createdat"]).ToString("dd-MM-yyyy hh:mm:ss tt"),
            //                 Details = new List<FormTransactionLine>() {
            //                     new FormTransactionLine {
            //                         FieldName = dr["fieldname"].ToString(),
            //                         OldValue = dr["oldvalue"].ToString(),
            //                         NewValue = dr["newvalue"].ToString()
            //                     }
            //                 }
            //             });
            //         }
            //     }

            //     return new GetAuditTrailResponse { Logs = logs };
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

            return new GetDesignHtmlResponse { Html = _temp };
        }

    }
}
